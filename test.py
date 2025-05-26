from Interface import rangepartition, getopenconnection
import time 

a = time.time()
conn = getopenconnection(dbname='dds_assgn1')
rangepartition('ratings', 10, conn)
conn.close()  
b = time.time() 

print(f"Time: {b - a:.2f}s") 