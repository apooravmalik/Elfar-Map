# server/services/latlong_service.py
from sqlalchemy import text
from config.database import SessionLocal
from utils.utils import map_color

def get_device_data():
    """
    Connects to the database, queries for device information,
    processes the data, and returns it.
    """
    db = SessionLocal()
    try:
        # UPDATED QUERY: Fetches dvcCurrentStateUser_TXT for color logic
        query = text("""
            SELECT
                dvcname_txt,
                dvcLatitude_DEC,
                dvcLongitude_DEC,
                dvcCurrentStateUser_TXT
            FROM
                device_tbl
            WHERE
                dvcCurrentStateUser_TXT LIKE 'Fence %'
                OR dvcCurrentStateUser_TXT LIKE '%axe_Elfar%'
            ORDER BY
                dvcname_txt
        """)

        result = db.execute(query)
        rows = result.fetchall()

        devices = []
        for row in rows:
            # 0: dvcname_txt, 1: dvcLatitude_DEC, 2: dvcLongitude_DEC,
            # 3: dvcCurrentStateUser_TXT
            status_text = row[3]
            devices.append({
                "name": row[0],
                "latitude": row[1],
                "longitude": row[2],
                "status": status_text,
                # Pass the status text to the map_color function
                "iconColor": map_color(status_text)
            })
        
        return devices

    except Exception as e:
        print(f"An error occurred while fetching device data: {e}")
        return {"error": str(e)}
    finally:
        db.close()
