import psycopg2
from dotenv import load_dotenv
import os 
from io import StringIO
from psycopg2.sql import SQL, Identifier, Literal
import multiprocessing

load_dotenv()

def getopenconnection(user='postgres', password='2324', dbname='postgres'):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=os.getenv("HOST")    
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
        SELECT userid, movieid, rating, ROW_NUMBER() OVER (ORDER BY userid) AS rnum
        FROM {};
    """).format(
        Identifier(ratingstablename)
    )
    try: 
        con = openconnection
        cur = con.cursor()
        
        save_rr_index(0)
        
        RROBIN_TABLE_PREFIX = 'rrobin_part'
        
        cur.execute(temp_tb)
        
        for i in range(0, numberofpartitions):
            table_name = RROBIN_TABLE_PREFIX + str(i)
            
            create_partition_table_sql = SQL("""
                CREATE TABLE {} (userid integer, movieid integer, rating float);
            """).format(Identifier(table_name))
            
            cur.execute(create_partition_table_sql)
            
            insert_into_partition_sql = SQL("""
                INSERT INTO {} (userid, movieid, rating) 
                SELECT userid, movieid, rating from temp 
                WHERE mod(temp.rnum - 1, {}) = {}; 
            """).format(
                Identifier(table_name),
                Literal(numberofpartitions),
                Literal(i) 
            )
            
            cur.execute(insert_into_partition_sql)
            
        con.commit() 
    except Exception as e:
        print(e) 
        if con:
            con.rollback()
        raise 
    finally:
        if cur: 
            # Xóa bảng `temp` nếu nó tồn tại
            try:
                cur.execute(SQL("DROP TABLE IF EXISTS temp;"))
            except Exception as drop_e:
                print(drop_e) 
                raise 
            
            cur.close() 

def count_partitions(prefix, openconnection): 
    con = None 
    cur = None 
    count = 0 
    try:
        con = openconnection
        cur = con.cursor() 
        
        like_pattern = prefix + '%'
        query = SQL("SELECT COUNT(*) FROM pg_stat_user_tables WHERE relname LIKE {};").format(Literal(like_pattern))
        
        cur.execute(query) 
        result = cur.fetchone()  
        if result:
            count = result[0]   
    except Exception as e:
        print(e)
        raise 
    finally:
        if cur:
            cur.close() 
    return count

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):

    con = None
    cur = None
    RANGE_TABLE_PREFIX = 'range_part'
    MAX_RATING_SCALE = 5.0  
    try:
        con = openconnection
        if con is None:
            raise ValueError("Database connection cannot be None.")
        
        if con.closed:
            raise ConnectionError("Database connection is closed.")

        cur = con.cursor()

        numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)

        if numberofpartitions <= 0:
            err_msg = (f"Error: No partitions found with prefix '{RANGE_TABLE_PREFIX}' "
                       f"or invalid count ({numberofpartitions}). Cannot insert.")
            raise ValueError(err_msg)

        delta = MAX_RATING_SCALE / numberofpartitions
        
        if rating <= 0.0:
            index = 0
        elif rating >= MAX_RATING_SCALE:
            index = numberofpartitions - 1
        else:
            index = int(rating / delta)
            if rating % delta == 0.0 and index > 0:
                index -= 1
        
        index = max(0, min(index, numberofpartitions - 1))
        partition_table_name_str = RANGE_TABLE_PREFIX + str(index)
        insert_data = (userid, itemid, rating)

        main_insert_sql = SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);").format(
            Identifier(ratingstablename)
        )
        cur.execute(main_insert_sql, insert_data)

        partition_insert_sql = SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);").format(
            Identifier(partition_table_name_str)
        )
        cur.execute(partition_insert_sql, insert_data)

        con.commit()
    except (ValueError, ConnectionError) as ve: # Catch specific custom errors
        print(f"Configuration or Connection Error: {ve}")
        # No rollback needed if transaction hasn't started or connection is bad
        raise
    except Exception as e:
        print(f"An error occurred during range insert: {e}")
        if con and not con.closed:
            try:
                con.rollback()
                print("Transaction rolled back due to error.")
            except Exception as rb_e:
                print(f"Error during rollback: {rb_e}")
        raise # Re-raise the original exception
    finally:
        if cur:
            try:
                cur.close()
            except Exception as e_close:
                print(f"Error closing cursor: {e_close}")

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
        # Bổ sung chèn hàng mới vào bảng chính 
        main_insert_sql = SQL("INSERT INTO {} (userid, movieid, rating) VALUES (%s, %s, %s);").format(
            Identifier(ratingstablename)
        )
        cur.execute(main_insert_sql, (userid, itemid, rating))
        
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%';")
        numberofpartitions = cur.fetchone()[0]

        current_index = get_rr_index()
        target_partition = current_index % numberofpartitions

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
