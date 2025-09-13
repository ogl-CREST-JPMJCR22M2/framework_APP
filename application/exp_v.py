### 1000回の更新トランザクションのうち，更新対象部品が更新される割合をPareto分布に従って決定

import numpy as np
import random
import polars as pl
import time
from concurrent.futures import ThreadPoolExecutor
from sqlalchemy import create_engine
from psycopg2 import connect, sql
from psycopg2.extras import execute_values

import SQLexecutor as SQLexe
import write_to_db as w
import varification as v
import calculation as c

# Set seed for reproducibility
np.random.seed(42)

num_total_parts = 30
num_transactions = 1000
#kaizan_percent = [1.0, 5.0, 10.0, 15.0] # %で
kaizan_percent = [1.0, 3.0, 5.0, 10.0]
percent = kaizan_percent[3]

#init calculation
root_partid = 'P0'
peers = ["postgresA", "postgresB", "postgresC"]
assembler = w.get_Assebler(root_partid)

start = time.time()
c.make_merkltree(assembler, root_partid)
t = time.time() - start

print("Calculation time:", t)

#"""
start = time.time()
result = v.valification(assembler, peers, root_partid)
t = time.time() - start
print("Varification time (Successflly):", t)
#"""

# kaizan part select 
parts = [f"P{i}" for i in range(num_total_parts)]

kaizantaisho = num_total_parts * percent * 0.01
if kaizantaisho < 1.0 : kaizantaisho = 1.0

per = random.sample(parts, int(kaizantaisho))

df_va = pl.DataFrame({
    "partid":per,
    "cfp": random.random()
})

engine = create_engine("postgresql://postgres:mysecretpassword@postgresA:5432/iroha_default")
sql_statement ="SELECT partid, assembler FROM partinfo;"
df = pl.read_database(sql_statement, engine)

df = df.join(df_va, on="partid", how="inner")


df_B = df.filter(pl.col("assembler") == "postgresB")
df_C = df.filter(pl.col("assembler") == "postgresC")

Blist = df_B.select(["partid", "cfp"]).rows()
Clist = df_C.select(["partid", "cfp"]).rows()

# offchain-dbへの書き込み
def kaizan(peer, lists):

    upsert_sql = """
        UPDATE cfpval AS t
        SET
            partid = v.partid,
            cfp = v.cfp
        FROM (VALUES %s)
        AS v(partid, cfp)
        WHERE v.partid = t.partid;
    """

    # DB接続
    conn = connect(
        dbname = "offchaindb", 
        user = "postgres", 
        password = "mysecretpassword",
        host = peer,
        port = 5432
    )

    with conn.cursor() as cur:
        execute_values(cur, upsert_sql, lists)

    conn.commit()
    conn.close()

kaizan("postgresB", Blist)
kaizan("postgresC", Clist)

start = time.time()
result = v.valification(assembler, peers, root_partid)
t = time.time() - start
print("Varification time:", t)

#print(result)
#print(per)
print("Specific rate:", len(result)/len(per)*100)

