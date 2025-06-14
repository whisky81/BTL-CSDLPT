import psycopg2

import os 
from io import StringIO
from psycopg2.sql import SQL, Identifier, Literal
import multiprocessing




def getopenconnection(dbname='postgres'):
    '''
    Connect to database 'dbname' through unix socket
    '''
    return psycopg2.connect(
        dbname=dbname,
        user=os.getenv("USER"), 
        password=os.getenv("PASSWORD"),
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
        cur = conn.cursor()

        # Tạo bảng nếu chưa tồn tại
        cur.execute(SQL("""
            CREATE TABLE IF NOT EXISTS {} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """).format(Identifier(ratings_table_name)))
        conn.commit()

        # Load dữ liệu theo batch
        batch_size = 200_000
        buffer = StringIO()

        with open(ratings_file_path, 'r') as file:
            for i, line in enumerate(file, 1):
                buffer.write(preprocess_line(line))

                if i % batch_size == 0:
                    buffer.seek(0)
                    copy_sql = SQL("""
                        COPY {} (userid, movieid, rating) 
                        FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\t')
                    """).format(Identifier(ratings_table_name))
                    cur.copy_expert(copy_sql, buffer)

                    buffer.seek(0)
                    buffer.truncate()

            # Insert phần còn lại chưa đến batch
            if buffer.tell():
                buffer.seek(0)
                copy_sql = SQL("""
                    COPY {} (userid, movieid, rating) 
                    FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\t')
                """).format(Identifier(ratings_table_name))
                cur.copy_expert(copy_sql, buffer)

        conn.commit()

    except (psycopg2.Error, IOError, Exception) as e:
        print("Error:", e)
        if conn:
            conn.rollback()
        raise

    finally:
        if cur:
            cur.close() 
# def loadratings(ratingstablename, ratingsfilepath, openconnection):
#     con = openconnection
#     cur = con.cursor()
#     cur.execute("create table " + ratingstablename + "(userid integer, extra1 char, movieid integer, extra2 char, rating float, extra3 char, timestamp bigint);")
#     cur.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
#     cur.execute("alter table " + ratingstablename + " drop column extra1, drop column extra2, drop column extra3, drop column timestamp;")
#     cur.close()
#     con.commit()


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



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    save_rr_index(0)
    RROBIN_TABLE_PREFIX = 'rrobin_part'
    cur = openconnection.cursor()

    # Create temporary table with row numbers
    temp_tb = SQL("""
        CREATE TEMPORARY TABLE temp AS 
        SELECT userid, movieid, rating, ROW_NUMBER() OVER (ORDER BY userid) AS rnum
        FROM {};
    """).format(Identifier(ratingstablename))
    cur.execute(temp_tb)

    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"

        # Create partition table
        cur.execute(SQL("""
            CREATE TABLE {} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """).format(Identifier(table_name)))

        # Insert into partition table using mod
        query = SQL("""
            INSERT INTO {} (userid, movieid, rating)
            SELECT userid, movieid, rating FROM temp
            WHERE MOD(temp.rnum - 1, %s) = %s;
        """).format(Identifier(table_name))
        cur.execute(query, (numberofpartitions, i))

    openconnection.commit()
    cur.close()


def count_partitions(prefix, openconnection):
        """
         Count number of tables starting with the given prefix.
        """
        cur = openconnection.cursor()
        cur.execute(
        SQL("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE {}").format(
            Literal(prefix + '%')
        )
    )
        count = cur.fetchone()[0]
        cur.close()
        return count

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    RANGE_TABLE_PREFIX = 'range_part'
    MAX_RATING_SCALE = 5.0

    cur = openconnection.cursor()
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)

    if numberofpartitions <= 0:
        raise ValueError("No range partitions found.")
    
    delta = MAX_RATING_SCALE / numberofpartitions
    index = min(numberofpartitions - 1, max(0, int(rating / delta)))

    if rating % delta == 0 and index > 0:
        index -= 1
    partition_table = f"{RANGE_TABLE_PREFIX}{index}"

    insert_sql = SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);")
    cur.execute(insert_sql.format(Identifier(ratingstablename)), (userid, itemid, rating))
    cur.execute(insert_sql.format(Identifier(partition_table)), (userid, itemid, rating))

    openconnection.commit()
    cur.close()


def get_rr_index():
    try:
        with open("rr_index.txt", 'r') as f:
            return int(f.read())
    except:
        return 0

def save_rr_index(index):
    with open("rr_index.txt", 'w') as f:
        f.write(str(index))

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%';")
        numberofpartitions = cur.fetchone()[0]

        current_index = get_rr_index()
        target_partition = current_index % numberofpartitions
        
        insert_sql = SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);")
        cur.execute(insert_sql.format(Identifier(ratingstablename)), (userid, itemid, rating))

        cur.execute(SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s)")
                    .format(Identifier("rrobin_part" + str(target_partition))),
                    (userid, itemid, rating))

        save_rr_index(current_index + 1)

        con.commit()
    except Exception as e:
        con.rollback()
        print("roundrobininsert failed:", e)
        raise
    finally:
        cur.close()