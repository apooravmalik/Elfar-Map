# server/services/cache.py
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from models.device_state import DeviceState
import os

# --- SQLite Cache Configuration (same as state_service.py) ---
CACHE_DIR = os.path.join(os.path.dirname(__file__), '..', 'cache')
CACHE_DB_PATH = os.path.join(CACHE_DIR, 'device_states.db')
CACHE_DATABASE_URL = f"sqlite:///{CACHE_DB_PATH}"

engine = create_engine(CACHE_DATABASE_URL, connect_args={"check_same_thread": False})
CacheSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_all_cached_devices():
    """
    Returns all devices in the cache with their current states.
    Useful for debugging what devices actually exist in the cache.
    """
    db = CacheSessionLocal()
    try:
        devices = db.query(DeviceState).all()
        return [
            {
                "dvcname_txt": device.dvcname_txt,
                "last_state": device.last_state,
                "effective_state": device.effective_state,
                "zone": device.zone,
                "line": device.line,
                "controller_id": device.controller_id,
                "device_type": device.device_type,
                "last_set_time": device.last_set_time.isoformat() if device.last_set_time else None,
                "updated_at": device.updated_at.isoformat() if device.updated_at else None
            }
            for device in devices
        ]
    finally:
        db.close()

def get_devices_by_controller_line(controller_id, line):
    """
    Returns all devices for a specific controller and line.
    This helps debug cascading failure scenarios.
    """
    db = CacheSessionLocal()
    try:
        devices = db.query(DeviceState).filter(
            DeviceState.controller_id == controller_id,
            DeviceState.line == line
        ).order_by(DeviceState.zone).all()
        
        return [
            {
                "dvcname_txt": device.dvcname_txt,
                "last_state": device.last_state,
                "effective_state": device.effective_state,
                "zone": device.zone,
                "line": device.line,
                "controller_id": device.controller_id,
                "device_type": device.device_type,
                "last_set_time": device.last_set_time.isoformat() if device.last_set_time else None
            }
            for device in devices
        ]
    finally:
        db.close()

def get_cache_statistics():
    """
    Returns statistics about the cache database.
    """
    db = CacheSessionLocal()
    try:
        total_devices = db.query(DeviceState).count()
        
        # Count by controller
        controllers = db.query(DeviceState.controller_id).distinct().all()
        controller_stats = []
        
        for (controller_id,) in controllers:
            if controller_id is not None:
                lines = db.query(DeviceState.line).filter(
                    DeviceState.controller_id == controller_id
                ).distinct().all()
                
                line_stats = []
                for (line,) in lines:
                    if line is not None:
                        zones_count = db.query(DeviceState).filter(
                            DeviceState.controller_id == controller_id,
                            DeviceState.line == line
                        ).count()
                        
                        zones = db.query(DeviceState.zone).filter(
                            DeviceState.controller_id == controller_id,
                            DeviceState.line == line
                        ).order_by(DeviceState.zone).all()
                        
                        zone_list = [z[0] for z in zones if z[0] is not None]
                        
                        line_stats.append({
                            "line": line,
                            "zone_count": zones_count,
                            "zones": zone_list
                        })
                
                controller_stats.append({
                    "controller_id": controller_id,
                    "total_devices": db.query(DeviceState).filter(
                        DeviceState.controller_id == controller_id
                    ).count(),
                    "lines": line_stats
                })
        
        # Count device types
        device_types = db.query(DeviceState.device_type).distinct().all()
        type_stats = []
        for (device_type,) in device_types:
            count = db.query(DeviceState).filter(
                DeviceState.device_type == device_type
            ).count()
            type_stats.append({
                "device_type": device_type,
                "count": count
            })
        
        return {
            "total_devices": total_devices,
            "controllers": controller_stats,
            "device_types": type_stats
        }
    finally:
        db.close()

def simulate_cascade_query(controller_id, line, fail_zone):
    """
    Simulates the exact query used in handle_fence_fail to see what devices would be affected.
    """
    db = CacheSessionLocal()
    try:
        devices_to_fail = db.query(DeviceState).filter(
            DeviceState.controller_id == controller_id,
            DeviceState.line == line,
            DeviceState.zone >= fail_zone
        ).all()
        
        return {
            "query_params": {
                "controller_id": controller_id,
                "line": line,
                "fail_zone": fail_zone
            },
            "devices_found": len(devices_to_fail),
            "devices": [
                {
                    "dvcname_txt": device.dvcname_txt,
                    "zone": device.zone,
                    "effective_state": device.effective_state,
                    "last_state": device.last_state
                }
                for device in devices_to_fail
            ]
        }
    finally:
        db.close()