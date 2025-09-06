# server/services/state_service.py
import os
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from config.database import SessionLocal as ProdSessionLocal
from models.device_state import Base, DeviceState

# --- SQLite Cache Configuration ---
CACHE_DIR = os.path.join(os.path.dirname(__file__), '..', 'cache')
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

CACHE_DB_PATH = os.path.join(CACHE_DIR, 'device_states.db')
CACHE_DATABASE_URL = f"sqlite:///{CACHE_DB_PATH}"

engine = create_engine(CACHE_DATABASE_URL, connect_args={"check_same_thread": False})
CacheSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

last_poll_time = None

# --- Parsing Logic ---
def parse_device_info(device_name, current_state):
    # Updated regex to match the actual format: "Fence Controller FC-1 Line 0 Zone Z1"
    name_pattern = re.compile(r'Fence Controller FC-(\d+)\s+Line\s+(\d+)\s+Zone\s+Z(\d+)')
    match = name_pattern.search(device_name)
    
    if match:
        controller_id, line, zone = map(int, match.groups())
        device_type = "axe_Elfar" if 'axe_Elfar' in current_state else "Fence Controller"
        zone_for_event = None if device_type == "axe_Elfar" else zone
        return {"zone": zone_for_event, "line": line, "controller_id": controller_id, "device_type": device_type}

    return {"zone": None, "line": None, "controller_id": None, "device_type": "Unknown"}

# --- Update Production DB Utility ---
def update_prod_db(prod_db, devices_to_update, axe_elfar_state_str=None):
    """
    Updates the dvcCurrentStateUser_TXT in the production database for a list of devices.
    """
    try:
        for device in devices_to_update:
            if axe_elfar_state_str:
                new_state_str = axe_elfar_state_str
            else:
                new_state_str = f"Fence {device.effective_state.capitalize()} {device.zone}_{device.line}_0_FC-{device.controller_id}"
                if device.device_type == 'axe_Elfar':
                    new_state_str = f"axe_Elfar{'Connected' if device.effective_state == 'Normal' else 'Disconnected'}"

            prod_db.execute(
                text("UPDATE device_tbl SET dvcCurrentStateUser_TXT = :state WHERE dvcname_txt = :name"),
                {"state": new_state_str, "name": device.dvcname_txt}
            )
        prod_db.commit()
        print(f"Successfully updated {len(devices_to_update)} devices in production DB.")
    except Exception as e:
        print(f"Error updating production DB: {e}")
        prod_db.rollback()


# --- Business Logic Handlers ---

def handle_fence_fail(db_session, changed_device):
    """Handles cascading failure for a Fence Controller."""
    print(f"HANDLING FENCE FAIL for {changed_device.dvcname_txt}")
    controller = changed_device.controller_id
    line = changed_device.line
    fail_zone = changed_device.zone

    # Find all zones on the same line at or after the fail zone
    devices_to_fail = db_session.query(DeviceState).filter(
        DeviceState.controller_id == controller,
        DeviceState.line == line,
        DeviceState.zone >= fail_zone
    ).all()

    for device in devices_to_fail:
        device.effective_state = 'Fail'
    
    return devices_to_fail

def handle_fence_normal(db_session, changed_device):
    """Handles line-wide recovery for a Fence Controller."""
    print(f"HANDLING FENCE NORMAL for {changed_device.dvcname_txt}")
    controller = changed_device.controller_id
    line = changed_device.line

    # Find all zones on the same line
    devices_to_normalize = db_session.query(DeviceState).filter(
        DeviceState.controller_id == controller,
        DeviceState.line == line
    ).all()

    for device in devices_to_normalize:
        device.effective_state = 'Normal'

    return devices_to_normalize

def handle_axe_elfar(db_session, changed_device):
    """
    Handles global state changes for axe_Elfar events.
    A disconnect triggers a system-wide fail-safe.
    """
    print(f"HANDLING GLOBAL AXE_ELFAR event for {changed_device.dvcname_txt}")
    new_state = 'Normal' if 'Connected' in changed_device.last_state else 'Fail'
    axe_elfar_state_str = f"axe_Elfar{'Connected' if new_state == 'Normal' else 'Disconnected'}"

    all_devices = db_session.query(DeviceState).all()

    for device in all_devices:
        device.effective_state = new_state
        device.last_state = axe_elfar_state_str

    return all_devices, axe_elfar_state_str


# --- Main Polling and Orchestration Logic ---

def poll_and_update_states():
    global last_poll_time
    prod_db = ProdSessionLocal()
    cache_db = CacheSessionLocal()
    
    print(f"--- Running poll job at {datetime.now()} ---")
    
    try:
        query = text("""
            SELECT dvcname_txt, dvcCurrentStateUser_TXT, dvcCurrentStateSetTime_DTM
            FROM device_tbl
            WHERE (dvcname_txt LIKE 'Fence Controller FC-%' OR dvcCurrentStateUser_TXT LIKE '%axe_Elfar%')
              AND dvcCurrentStateSetTime_DTM > :last_poll_time
            ORDER BY dvcCurrentStateSetTime_DTM ASC
        """)
        
        changed_devices_from_prod = prod_db.execute(query, {"last_poll_time": last_poll_time}).fetchall()

        if not changed_devices_from_prod:
            print("No new device state changes detected.")
            return

        latest_timestamp_in_batch = last_poll_time
        for device_name, current_state, set_time in changed_devices_from_prod:
            print(f"DEBUG: Processing device {device_name} with state {current_state} at {set_time}")
            
            cached_device = cache_db.query(DeviceState).filter_by(dvcname_txt=device_name).first()
            if not cached_device:
                print(f"WARNING: Device {device_name} not found in cache!")
                continue
                
            if cached_device.last_state == current_state:
                print(f"DEBUG: No state change for {device_name} - skipping")
                continue # Skip if no actual change

            print(f"DEBUG: State changed from '{cached_device.last_state}' to '{current_state}'")

            # --- Update cache with the new raw state FIRST ---
            parsed_info = parse_device_info(device_name, current_state)
            print(f"DEBUG: Parsed info: {parsed_info}")
            
            cached_device.last_state = current_state
            cached_device.last_set_time = set_time
            for key, value in parsed_info.items():
                setattr(cached_device, key, value)
            
            # --- ORCHESTRATE BUSINESS LOGIC ---
            devices_to_update_in_prod = []
            axe_elfar_state_str = None

            if parsed_info["device_type"] == "axe_Elfar":
                print(f"DEBUG: Handling axe_Elfar event")
                devices_to_update_in_prod, axe_elfar_state_str = handle_axe_elfar(cache_db, cached_device)
            
            elif 'Fail' in current_state:
                print(f"DEBUG: Handling Fence Fail event")
                devices_to_update_in_prod = handle_fence_fail(cache_db, cached_device)
                print(f"DEBUG: handle_fence_fail returned {len(devices_to_update_in_prod)} devices")
            
            elif 'Normal' in current_state:
                print(f"DEBUG: Handling Normal event")
                # Check if the line was previously failed to trigger recovery
                line_was_failed = any(
                    d.effective_state == 'Fail' for d in cache_db.query(DeviceState).filter_by(
                        controller_id=cached_device.controller_id, line=cached_device.line
                    )
                )
                print(f"DEBUG: Line was previously failed: {line_was_failed}")
                if line_was_failed:
                    devices_to_update_in_prod = handle_fence_normal(cache_db, cached_device)
                    print(f"DEBUG: handle_fence_normal returned {len(devices_to_update_in_prod)} devices")
                else: # Simple normal update
                    cached_device.effective_state = 'Normal'
                    print(f"DEBUG: Simple normal update for {device_name}")
            
            elif 'Alarm' in current_state:
                print(f"DEBUG: Handling Alarm event")
                cached_device.effective_state = 'Alarm'
                # No cascading, so no prod update needed for others

            # --- Commit changes to Prod DB ---
            print(f"DEBUG: About to update {len(devices_to_update_in_prod)} devices in prod DB")
            if devices_to_update_in_prod:
                try:
                    update_prod_db(prod_db, devices_to_update_in_prod, axe_elfar_state_str)
                except Exception as e:
                    print(f"ERROR in update_prod_db: {e}")
                    raise

            if set_time > latest_timestamp_in_batch:
                latest_timestamp_in_batch = set_time
        
        cache_db.commit() # Commit all cache changes at the end
        # Add a small increment to avoid processing the same timestamp twice
        last_poll_time = latest_timestamp_in_batch + timedelta(microseconds=1)
        print(f"Successfully processed {len(changed_devices_from_prod)} state changes. New last_poll_time: {last_poll_time}")

    except Exception as e:
        print(f"An error occurred during polling: {e}")
        cache_db.rollback()
    finally:
        prod_db.close()
        cache_db.close()

# --- Other functions (initialize_cache_db, get_all_device_states) are unchanged ---
def initialize_cache_db():
    global last_poll_time
    Base.metadata.create_all(bind=engine)
    
    cache_db = CacheSessionLocal()
    try:
        is_empty = cache_db.query(DeviceState).first() is None
        if is_empty:
            print("Cache is empty. Performing initial backfill...")
            prod_db = ProdSessionLocal()
            try:
                backfill_query = text("""
                    SELECT dvcname_txt, dvcCurrentStateUser_TXT, dvcCurrentStateSetTime_DTM
                    FROM device_tbl
                    WHERE (dvcname_txt LIKE 'Fence Controller FC-%' OR dvcCurrentStateUser_TXT LIKE '%axe_Elfar%')
                """)
                devices = prod_db.execute(backfill_query).fetchall()

                if not devices:
                    last_poll_time = datetime.now() - timedelta(minutes=5)
                    return

                latest_timestamp = datetime.min
                for device_name, current_state, set_time in devices:
                    # Note: We are not applying cascading rules on initial backfill, just a direct state mapping.
                    effective_state = "Normal" if "Normal" in current_state or "Connected" in current_state else "Fail"
                    if "Alarm" in current_state: effective_state = "Alarm"
                    
                    parsed_info = parse_device_info(device_name, current_state)

                    new_device = DeviceState(
                        dvcname_txt=device_name,
                        last_state=current_state,
                        effective_state=effective_state,
                        last_set_time=set_time,
                        **parsed_info
                    )
                    cache_db.add(new_device)
                    if set_time and set_time > latest_timestamp:
                        latest_timestamp = set_time
                
                cache_db.commit()
                last_poll_time = latest_timestamp
                print(f"Backfill complete. Populated {len(devices)} devices. Last poll time: {last_poll_time}")
            finally:
                prod_db.close()
        else:
            max_time = cache_db.query(func.max(DeviceState.last_set_time)).scalar()
            last_poll_time = max_time or (datetime.now() - timedelta(minutes=5))
            print(f"Initialized last_poll_time from cache: {last_poll_time}")
    finally:
        cache_db.close()

def get_all_device_states():
    db = CacheSessionLocal()
    try:
        states = db.query(DeviceState).all()
        return [
            {
                "name": state.dvcname_txt,
                "last_state": state.last_state,
                "effective_state": state.effective_state,
                "color": "blue" if state.effective_state == "Normal" else "red",
                "last_set_time": state.last_set_time.isoformat() if state.last_set_time else None,
                "updated_at": state.updated_at.isoformat(),
                "zone": state.zone,
                "line": state.line,
                "controller_id": state.controller_id,
                "device_type": state.device_type
            } for state in states
        ]
    finally:
        db.close()