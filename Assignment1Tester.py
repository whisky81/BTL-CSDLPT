#
# Tester for the assignement1
#
DATABASE_NAME = 'dds_assgn1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = './ml-10M100K/ratings.dat'
ACTUAL_ROWS_IN_INPUT_FILE = 10_000_054  # Number of lines in the input file

import time 
from termcolor import colored
import psycopg2
import traceback
import testHelper
import Interface as MyAssignment

exe_time = []

def execution_time(start, end, func):
    tmp = f"Execution Time of {func}: {end - start} s"
    # print(colored(tmp, "green"))
    exe_time.append(tmp) 



if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            start = time.time()
            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
            end = time.time()
            if result :
                print("loadratings function pass!")
            else:
                print("loadratings function fail!")
            
            execution_time(start, end, 'loadratings')

            start = time.time()
            [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            end = time.time()
            if result :
                print("rangepartition function pass!")
            else:
                print("rangepartition function fail!")
            
            execution_time(start, end, 'rangepartition')


            
            start = time.time()
            # ALERT:: Use only one at a time i.e. uncomment only one line at a time and run the script
            # [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
            [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 0, conn, '0')
            end = time.time()
            if result:
                print("rangeinsert function pass!")
            else:
                print("rangeinsert function fail!")
            
            execution_time(start, end, 'rangeinsert')

            testHelper.deleteAllPublicTables(conn)
            MyAssignment.loadratings(RATINGS_TABLE, INPUT_FILE_PATH, conn)
            
            start = time.time()
            [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
            end = time.time()
            if result :
                print("roundrobinpartition function pass!")
            else:
                print("roundrobinpartition function fail")
            
            execution_time(start, end, 'roundrobinpartition')

            start = time.time()
            # ALERT:: Change the partition index according to your testing sequence.
            [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '1')
            # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '2')
            end = time.time()
            if result :
                print("roundrobininsert function pass!")
            else:
                print("roundrobininsert function fail!")
            
            execution_time(start, end, 'roundrobininsert')

            choice = input('Press enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
            if not conn.close:
                conn.close() 
            
            for et in exe_time:
                print(colored(et, "green"))

    except Exception as detail:
        traceback.print_exc()