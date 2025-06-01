from Interface import getopenconnection, roundrobinpartition 
import os 
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    conn = None 
    try:
        conn = getopenconnection(dbname=os.getenv("DATABASE_NAME"))
        roundrobinpartition(
            'ratings',
            10,
            conn 
        )
    except Exception as e:
        print(e)
        raise 
    finally:
        conn.close()
        