
import time
import calculation as c
import write_to_db as w
import update as u
import varification as v
import SQLexecutor as SQLexe
import random


root_partid = 'P0'
peers = ["postgresA", "postgresB", "postgresC"]
assembler_root = peers[0]
c.make_merkltree(assembler_root, root_partid)

print("deepth:")
w.get_hash()
print("time:")

time_v_s = []
time_v_f = []
time_u = []

for i in range(5):

    target_partid = "P"+str(random.randint(0, 2999))
    assembler = w.get_Assebler(target_partid)
    #c.make_merkltree(assembler_root, root_partid)

    start = time.time()
    v.valification(assembler_root, peers, root_partid)
    t = time.time() - start
    time_v_s.append(t)

    sql = f"UPDATE cfpval SET cfp = '0.1717' WHERE partid = '{target_partid}';"
    SQLexe.COMMANDexecutor_off(sql, assembler)
    start = time.time()
    v.valification(assembler_root, peers, root_partid)
    t = time.time() - start
    time_v_f.append(t)

    start = time.time()
    u.make_merkltree(assembler, target_partid, 0.1717)
    t = time.time() - start
    time_u.append(t)
    
for t in time_u:
    print(t)

print("\n")

for t in time_v_s:
    print(t)

print("\n")

for t in time_v_f:
    print(t)

print("\n")

   
    
    
        