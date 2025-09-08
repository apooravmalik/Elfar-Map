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
    """
    Fixed parsing logic that always maintains device_type as "Fence Controller" for fence devices.
    Business logic decisions should be based on state content, not device_type.
    """
    name_pattern = re.compile(r'Fence Controller FC-(\d+)\s+Line\s+(\d+)\s+Zone\s+Z(\d+)')
    match = name_pattern.search(device_name)
    
    if match:
        controller_id, line, zone = map(int, match.groups())
        # ALWAYS maintain device_type as "Fence Controller" for all fence devices
        # The state content will determine business logic behavior, not the device_type
        device_type = "Fence Controller"
        return {"zone": zone, "line": line, "controller_id": controller_id, "device_type": device_type}

    return {"zone": None, "line": None, "controller_id": None, "device_type": "Unknown"}

def is_axe_elfar_state(state_string):
    """Helper function to determine if a state represents an axe_Elfar event"""
    return 'axe_Elfar' in state_string

def is_fence_fail_state(state_string):
    """Helper function to determine if a state represents a fence fail event"""
    return 'Fence' in state_string and 'Fail' in state_string

def is_fence_normal_state(state_string):
    """Helper function to determine if a state represents a fence normal event"""
    return 'Fence' in state_string and 'Normal' in state_string

def is_alarm_state(state_string):
    """Helper function to determine if a state represents an alarm event"""
    return 'Alarm' in state_string

# --- Update Production DB Utility ---
def update_prod_db(prod_db, devices_to_update):
    """
    Updates the dvcCurrentStateUser_TXT in the production database for a list of devices.
    Now generates state strings based on the device's last_state which always represents
    what should be in the production DB.
    """
    try:
        for device in devices_to_update:
            new_state_str = generate_production_state_string(device)
            
            prod_db.execute(
                text("UPDATE device_tbl SET dvcCurrentStateUser_TXT = :state WHERE dvcname_txt = :name"),
                {"state": new_state_str, "name": device.dvcname_txt}
            )
        prod_db.commit()
        print(f"Successfully updated {len(devices_to_update)} devices in production DB.")
    except Exception as e:
        print(f"Error updating production DB: {e}")
        prod_db.rollback()

def generate_production_state_string(device):
    """
    Generates the correct production state string based on the device's last_state.
    Since last_state now always represents what should be in production DB,
    we can simply return it directly.
    """
    # last_state now always represents the current intended production state
    return device.last_state

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
        # ALWAYS update last_state to reflect current fence fail state
        # This ensures last_state represents what should be in production DB
        device.last_state = f"Fence Fail {device.zone}_{device.line}_0_FC-{device.controller_id}"

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
        # ALWAYS update last_state to reflect current fence normal state
        # This ensures last_state represents what should be in production DB
        device.last_state = f"Fence Normal {device.zone}_{device.line}_0_FC-{device.controller_id}"

    return devices_to_normalize

def handle_axe_elfar_global_event(db_session, changed_device):
    """
    Handles global state changes for axe_Elfar events.
    A disconnect triggers a system-wide fail-safe.
    Note: This does NOT change device_type, only states.
    """
    print(f"HANDLING GLOBAL AXE_ELFAR event for {changed_device.dvcname_txt}")
    new_effective_state = 'Normal' if 'Connected' in changed_device.last_state else 'Fail'
    axe_elfar_state_str = f"axe_Elfar{'Connected' if new_effective_state == 'Normal' else 'Disconnected'}"

    all_devices = db_session.query(DeviceState).all()

    for device in all_devices:
        device.effective_state = new_effective_state
        # Set ALL devices to axe_Elfar state during global event
        device.last_state = axe_elfar_state_str
        # IMPORTANT: device_type remains "Fence Controller" - we don't change it!

    return all_devices

def handle_fence_alarm(db_session, changed_device):
    """Handles alarm state for a Fence Controller."""
    print(f"HANDLING FENCE ALARM for {changed_device.dvcname_txt}")
    changed_device.effective_state = 'Alarm'
    # Update last_state to reflect current fence alarm state
    changed_device.last_state = f"Fence Alarm {changed_device.zone}_{changed_device.line}_0_FC-{changed_device.controller_id}"
    # No cascading for alarms, just return the single device
    return [changed_device]

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

            # Check if we should skip this update
            if cached_device.last_state == current_state and not (is_fence_fail_state(current_state) and cached_device.effective_state != 'Fail'):
                print(f"DEBUG: No state change for {device_name} - skipping")
                continue

            if is_fence_fail_state(current_state) and cached_device.effective_state != 'Fail':
                print(f"DEBUG: Forcing fail event for {device_name} because effective_state is '{cached_device.effective_state}'")

            print(f"DEBUG: State changed from '{cached_device.last_state}' to '{current_state}'")

            # --- Update cache with the new raw state FIRST ---
            parsed_info = parse_device_info(device_name, current_state)
            print(f"DEBUG: Parsed info: {parsed_info}")

            cached_device.last_state = current_state
            cached_device.last_set_time = set_time
            for key, value in parsed_info.items():
                setattr(cached_device, key, value)

            # --- ORCHESTRATE BUSINESS LOGIC BASED ON STATE CONTENT, NOT DEVICE_TYPE ---
            devices_to_update_in_prod = []

            # Route based on state content, not device_type
            if is_axe_elfar_state(current_state):
                print(f"DEBUG: Handling axe_Elfar global event based on state content")
                devices_to_update_in_prod = handle_axe_elfar_global_event(cache_db, cached_device)

            elif is_fence_fail_state(current_state):
                print(f"DEBUG: Handling Fence Fail event")
                devices_to_update_in_prod = handle_fence_fail(cache_db, cached_device)
                # The triggering device is already included in the list from handle_fence_fail

            elif is_fence_normal_state(current_state):
                print(f"DEBUG: Handling Fence Normal event")
                # Check if the line was previously failed to trigger recovery
                line_was_failed = any(
                    d.effective_state == 'Fail' for d in cache_db.query(DeviceState).filter_by(
                        controller_id=cached_device.controller_id, line=cached_device.line
                    )
                )
                print(f"DEBUG: Line was previously failed: {line_was_failed}")
                if line_was_failed:
                    devices_to_update_in_prod = handle_fence_normal(cache_db, cached_device)
                else:
                    # Simple normal update
                    cached_device.effective_state = 'Normal'
                    # Update last_state to reflect current fence normal state
                    cached_device.last_state = f"Fence Normal {cached_device.zone}_{cached_device.line}_0_FC-{cached_device.controller_id}"
                    devices_to_update_in_prod = [cached_device]

            elif is_alarm_state(current_state):
                print(f"DEBUG: Handling Alarm event")
                devices_to_update_in_prod = handle_fence_alarm(cache_db, cached_device)

            else:
                print(f"DEBUG: Unknown state type: {current_state}")
                # Handle unknown states gracefully
                devices_to_update_in_prod = [cached_device]

            # --- Commit changes to Prod DB ---
            print(f"DEBUG: About to update {len(devices_to_update_in_prod)} devices in prod DB")
            if devices_to_update_in_prod:
                try:
                    update_prod_db(prod_db, devices_to_update_in_prod)
                except Exception as e:
                    print(f"ERROR in update_prod_db: {e}")
                    raise

            if set_time > latest_timestamp_in_batch:
                latest_timestamp_in_batch = set_time

        cache_db.commit()  # Commit all cache changes at the end
        # Add a small increment to avoid processing the same timestamp twice
        last_poll_time = latest_timestamp_in_batch + timedelta(microseconds=1)
        print(f"Successfully processed {len(changed_devices_from_prod)} state changes. New last_poll_time: {last_poll_time}")

    except Exception as e:
        print(f"An error occurred during polling: {e}")
        cache_db.rollback()
    finally:
        prod_db.close()
        cache_db.close()

# --- Initialization and Utility Functions ---

def initialize_cache_db():
    """Initialize cache database with proper device_type classification"""
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
                    # Map effective state based on current state content
                    if is_fence_normal_state(current_state) or (is_axe_elfar_state(current_state) and 'Connected' in current_state):
                        effective_state = "Normal"
                    elif is_fence_fail_state(current_state) or (is_axe_elfar_state(current_state) and 'Disconnected' in current_state):
                        effective_state = "Fail"
                    elif is_alarm_state(current_state):
                        effective_state = "Alarm"
                    else:
                        effective_state = "Normal"  # Default
                    
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
            # Fix existing cache: reset all device_types to "Fence Controller"
            print("Fixing existing cache: resetting device_types to 'Fence Controller'")
            fence_devices = cache_db.query(DeviceState).filter(
                DeviceState.dvcname_txt.like('Fence Controller FC-%')
            ).all()
            
            for device in fence_devices:
                device.device_type = "Fence Controller"
            
            cache_db.commit()
            print(f"Fixed {len(fence_devices)} devices in cache")
            
            max_time = cache_db.query(func.max(DeviceState.last_set_time)).scalar()
            last_poll_time = max_time or (datetime.now() - timedelta(minutes=5))
            print(f"Initialized last_poll_time from cache: {last_poll_time}")
    finally:
        cache_db.close()

def get_all_device_states():
    """Get all device states with consistent device_type"""
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
                "device_type": state.device_type  # This should now consistently be "Fence Controller"
            } for state in states
        ]
    finally:
        db.close()