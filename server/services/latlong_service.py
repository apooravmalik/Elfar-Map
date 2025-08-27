from sqlalchemy import text
from config.database import SessionLocal
from utils.utils import map_color

def get_device_data():
    """
    Connects to the database, queries for device information,
    processes the data, and returns it.
    """
    # Get a new database session
    db = SessionLocal()
    try:
        query = text("""
            SELECT
                dvcname_txt,
                dvcLatitude_DEC,
                dvcLongitude_DEC,
                dvcIconColourPriority_FRK
            FROM
                device_tbl
            WHERE
                dvcCurrentStateUser_TXT LIKE 'Fence %'
                OR dvcCurrentStateUser_TXT LIKE '%axe_Elfar%'
            ORDER BY
                dvcname_txt
        """)

        # Execute the query and fetch all results
        result = db.execute(query)
        rows = result.fetchall()

        devices = []
        # Process each row from the database result.
        for row in rows:
            # The result from SQLAlchemy 2.0 is a Row object which is like a tuple
            # but also allows access by column name. We'll use integer indexes here.
            # 0: dvcname_txt, 1: dvcLatitude_DEC, 2: dvcLongitude_DEC,
            # 3: dvcIconColourPriority_FRK
            devices.append({
                "name": row[0],
                "latitude": row[1],
                "longitude": row[2],
                "colorPriority": row[3],
                # Use the utility function to map priority to a color string
                "iconColor": map_color(row[3])
            })
        
        return devices

    except Exception as e:
        # Log the error for debugging purposes
        print(f"An error occurred while fetching device data: {e}")
        # Return an empty list or an error structure if something goes wrong
        return {"error": str(e)}
    finally:
        # IMPORTANT: Always close the database session
        db.close()

