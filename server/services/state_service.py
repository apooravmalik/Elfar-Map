# server/services/state_service.py
import os
import re
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text, func
from sqlalchemy.orm import sessionmaker
from config.database import SessionLocal as ProdSessionLocal
from models.device_state import Base, DeviceState

# --- SQLite Cache Configuration (Unchanged) ---
CACHE_DIR = os.path.join(os.path.dirname(__file__), '..', 'cache')
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

CACHE_DB_PATH = os.path.join(CACHE_DIR, 'device_states.db')
CACHE_DATABASE_URL = f"sqlite:///{CACHE_DB_PATH}"

engine = create_engine(CACHE_DATABASE_URL, connect_args={"check_same_thread": False})
CacheSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

last_poll_time = None

# --- Updated Parsing Logic ---
def parse_device_info(device_name, current_state):
    """
    Parses zone, line, and controller ID exclusively from the device name.
    The current_state is only used to determine the device type.
    """
    # This is now the single source of truth for identity.
    # It handles names like: "Fence Controller FC-14 Line 0 Zone Z22"
    name_pattern = re.compile(r'FC-(\d+)\s+Line\s+(\d+)\s+Zone\s+Z(\d+)')

    match = name_pattern.search(device_name)
    
    if match:
        controller_id, line, zone = map(int, match.groups())
        
        # Determine the device type from the state message, not the name.
        device_type = "axe_Elfar" if 'axe_Elfar' in current_state else "Fence Controller"
        
        # If it's an axe_Elfar event, the concept of a "zone" is irrelevant for that event.
        # The identity (controller, line) is still correctly parsed from the name.
        zone_for_event = None if device_type == "axe_Elfar" else zone

        return {
            "zone": zone_for_event,
            "line": line,
            "controller_id": controller_id,
            "device_type": device_type
        }

    # Fallback for any device names that don't match the standard pattern
    return {
        "zone": None, "line": None, "controller_id": None, "device_type": "Unknown"
    }


# --- Business Logic (Unchanged) ---
def apply_business_rules(device_name, current_state):
    if 'axe_Elfar' in current_state:
        return 'Normal' if 'Connected' in current_state else 'Fail'
    elif 'Fence Controller' in device_name or 'Fence ' in current_state:
        if 'Alarm' in current_state: return 'Alarm'
        if 'Fail' in current_state: return 'Fail'
        if 'Normal' in current_state: return 'Normal'
    return 'Unknown'

# --- Database Initialization (Unchanged from last version) ---
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
                    effective_state = apply_business_rules(device_name, current_state)
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

# --- Polling Logic (Unchanged from last version) ---
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
        
        devices = prod_db.execute(query, {"last_poll_time": last_poll_time}).fetchall()

        if not devices:
            print("No new device state changes detected.")
            return

        latest_timestamp_in_batch = last_poll_time
        for device_name, current_state, set_time in devices:
            effective_state = apply_business_rules(device_name, current_state)
            parsed_info = parse_device_info(device_name, current_state)

            cached_device = cache_db.query(DeviceState).filter_by(dvcname_txt=device_name).first()

            if cached_device:
                cached_device.last_state = current_state
                cached_device.effective_state = effective_state
                cached_device.last_set_time = set_time
                for key, value in parsed_info.items():
                    setattr(cached_device, key, value)
            else:
                new_device = DeviceState(
                    dvcname_txt=device_name,
                    last_state=current_state,
                    effective_state=effective_state,
                    last_set_time=set_time,
                    **parsed_info
                )
                cache_db.add(new_device)
            
            if set_time > latest_timestamp_in_batch:
                latest_timestamp_in_batch = set_time
        
        cache_db.commit()
        last_poll_time = latest_timestamp_in_batch
        print(f"Successfully updated states for {len(devices)} devices. New last_poll_time: {last_poll_time}")

    except Exception as e:
        print(f"An error occurred during polling: {e}")
        cache_db.rollback()
    finally:
        prod_db.close()
        cache_db.close()
        
# --- API Endpoint Data (Unchanged from last version) ---
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