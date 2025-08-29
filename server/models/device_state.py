# server/models/device_state.py
from sqlalchemy import Column, String, DateTime, func, Integer
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class DeviceState(Base):
    """
    SQLAlchemy model for the device_state_cache table.
    Enriched with parsed columns for efficient querying.
    """
    __tablename__ = 'device_state_cache'

    dvcname_txt = Column(String, primary_key=True)
    last_state = Column(String)
    effective_state = Column(String)
    last_set_time = Column(DateTime)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # --- New Parsed Columns ---
    zone = Column(Integer)
    line = Column(Integer)
    controller_id = Column(Integer)
    device_type = Column(String) # e.g., 'Fence Controller' or 'axe_Elfar'

    def __repr__(self):
        return (
            f"<DeviceState(dvcname_txt='{self.dvcname_txt}', "
            f"zone='{self.zone}', line='{self.line}', controller='{self.controller_id}')>"
        )