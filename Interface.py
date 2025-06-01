import psycopg2
from dotenv import load_dotenv
import os 
from io import StringIO
from psycopg2.sql import SQL, Identifier
import multiprocessing

load_dotenv()

def getopenconnection(user='postgres', password='2324', dbname='postgres'):
    '''
    Connect to database 'dbname' through unix socket
    '''
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

        batch_size=150_000
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

def insert_partition(args):
    '''
    Helper func for rangepartition 
    '''
    i, delta, ratings_table_name = args
    conn = getopenconnection(dbname=os.getenv("DATABASE_NAME"))
    cur = conn.cursor()

    minRange = i * delta
    maxRange = minRange + delta
    table_name = f"range_part{i}"
    if i == 0:
        query = f"INSERT INTO {table_name} SELECT userid, movieid, rating FROM {ratings_table_name} WHERE rating >= {minRange} AND rating <= {maxRange};"
    else:
        query = f"INSERT INTO {table_name} SELECT userid, movieid, rating FROM {ratings_table_name} WHERE rating > {minRange} AND rating <= {maxRange};"

    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()

# 8.33s
def rangepartition(ratings_table_name, number_of_partitions, open_connection):
    cur = open_connection.cursor()
    delta = 5 / number_of_partitions
    for i in range(number_of_partitions):
        table_name = f"range_part{i}"
        cur.execute(f"CREATE TABLE {table_name} (userid INTEGER, movieid INTEGER, rating FLOAT);")
    open_connection.commit()
    cur.close()

    args = [(i, delta, ratings_table_name) for i in range(number_of_partitions)]
    with multiprocessing.Pool() as pool:
        pool.map(insert_partition, args)

# 14s
# def rangepartition(ratingstablename, numberofpartitions, openconnection):
#     """
#     Function to create partitions of main table based on range of ratings.
#     """
#     con = openconnection
#     cur = con.cursor()
#     delta = 5 / numberofpartitions
#     RANGE_TABLE_PREFIX = 'range_part'
#     for i in range(0, numberofpartitions):
#         minRange = i * delta
#         maxRange = minRange + delta
#         table_name = RANGE_TABLE_PREFIX + str(i)
#         cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
#         if i == 0:
#             cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(minRange) + " and rating <= " + str(maxRange) + ";")
#         else:
#             cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(minRange) + " and rating <= " + str(maxRange) + ";")
#     cur.close()
#     con.commit()

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    Thay thế  mod(temp.rnum-1, 5) -> mod(temp.rnum - 1, numberofpartitions) để  khỏi mất mát dữ liệu trong quá trình phân mảnh khi numberofpartition khác 5 
    """
    con = None 
    cur = None
    
    # tạo bảng temp để tránh tạo bảng lặp đi lặp lại trong for loop 
    temp_tb = SQL("""
        CREATE TEMPORARY TABLE temp AS 
        SELECT userid, movieid, rating, ROW_NUMBER() OVER () AS rnum
        FROM {};
    """).format(
        Identifier(ratingstablename)
    )
    try: 
        con = openconnection
        cur = con.cursor()
        RROBIN_TABLE_PREFIX = 'rrobin_part'
        
        cur.execute(temp_tb)
        
        for i in range(0, numberofpartitions):
            table_name = RROBIN_TABLE_PREFIX + str(i)
            # cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")
            cur.execute(SQL("""
                    CREATE TABLE {} (userid integer, movieid integer, rating float);
                """).format(Identifier(table_name)))
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from temp where mod(temp.rnum-1, " + str(numberofpartitions) + ") = " + str(i) + ";")
            
        
        con.commit() 
    except Exception as e:
        print(e) 
        raise 
    finally:
        if con and cur: 
            cur.close()
        