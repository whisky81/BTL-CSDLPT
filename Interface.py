import psycopg2
from dotenv import load_dotenv
import os 
from io import StringIO
from psycopg2.sql import SQL, Identifier

load_dotenv()

def getopenconnection(user='postgres', password='2324', dbname='postgres'):
    return psycopg2.connect(
        dbname=dbname,
        user=user, 
        password=password,
        host=os.getenv("HOST"),
        port=os.getenv("PORT")
    )

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    conn = None
    cur = None 
    try:
        conn = getopenconnection(dbname='postgres') 
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Check if database exists (proper parameterized query)
        cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (dbname,))
        
        if not cur.fetchone():
            cur.execute(SQL("CREATE DATABASE {}").format(Identifier(dbname)))
            print(f"Database '{dbname}' created successfully")
        else:
            print(f"Database '{dbname}' already exists")
        
    except psycopg2.Error as e:
        print(e)
        raise
    finally:
        if cur: cur.close() 
        if conn: conn.close() 

# helper func
def preprocess_line(line):
    parts = line.strip().split("::")
    return f"{parts[0]}\t{parts[1]}\t{parts[2]}\n"

# best time: 7.82s
def loadratings(ratings_table_name, ratings_file_path, open_connection):
    conn = open_connection 
    cur = None 
    try:
        # create_db(dbname=os.getenv("DATABASE_NAME")) 
        cur = conn.cursor()

        cur.execute(SQL("CREATE TABLE IF NOT EXISTS {} (userid INTEGER, movieid INTEGER, rating FLOAT)").format(Identifier(ratings_table_name))) 
        conn.commit()

        batch_size=200_000
        buffer = StringIO()
        with open(ratings_file_path, 'r') as file:
            for i, line in enumerate(file, 1):
                buffer.write(preprocess_line(line))
                if i % batch_size == 0:
                    buffer.seek(0)
                    cur.copy_from(buffer, 'ratings', sep='\t', columns=('userid', 'movieid', 'rating'))

                    buffer.seek(0) 
                    buffer.truncate() 

            if buffer.tell():
                buffer.seek(0)
                cur.copy_from(buffer, 'ratings', sep='\t', columns=('userid', 'movieid', 'rating'))
       
        conn.commit()
    except psycopg2.Error as e:
        print(e) 
        if conn: conn.rollback()
        raise 
    except IOError as e:
        print(e) 
        raise 
    except Exception as e:
        print(e) 
        if conn: conn.rollback()
        raise 
    finally:
        if cur: cur.close()