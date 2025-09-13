### partidを使って求める

from sqlalchemy import create_engine, text
import polars as pl
import time
import hashlib
from decimal import *
import zlib
from typing import Optional
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
from psycopg2._psycopg import connection, cursor
from collections import defaultdict

import SQLexecutor as SQLexe
import write_to_db as w


# ======== DataFrameの表示の仕方 ======== #
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_fmt_str_lengths(n=30)
# ===================================== #


def valification(peer, peers, root_partid):

    conn: Optional[connection] = None
    try:
        dsn = {
            "dbname": "iroha_default",
            "user": "postgres",
            "password": "mysecretpassword",
            "port": "5432",
            "host": peer
        }
        conn = connect(**dsn)
        conn.autocommit = True

        with conn.cursor() as cur:

            ## 一時テーブルの構築
            cur.execute("""
                CREATE TEMP TABLE temp (
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    assembler CHARACTER varying(288),
                    cfp DECIMAL, 
                    co2 DECIMAL,
                    PRIMARY KEY (partid)
                );

                CREATE TEMP TABLE hashvals(
                    partid CHARACTER varying(288),
                    can_hashing boolean,
                    hash bytea[],
                    PRIMARY KEY (partid)
                );

                CREATE INDEX idx_temp ON temp(partid);
                CREATE INDEX idx_hash ON hashvals(partid);
                CREATE INDEX idx_hash_temp ON hashvals(hash);
                
            """)
            
            ## cfpの算出
            # offchain-dbからcfpの算出
            co2_import = " UNION ALL \n".join(["SELECT * FROM dblink('host="+ p +" port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 'SELECT partid, cfp FROM cfpval') AS t1(partid CHARACTER varying(288), cfp DECIMAL)" 
            for p in peers ]) 
        
            sql_ = f"""
                INSERT INTO temp (partid, parents_partid,  assembler, cfp) 
                WITH cfpval AS (
                    {co2_import}
                ),
                part_tree AS( 
                    WITH RECURSIVE calc(partid, parents_partid) AS 
                        ( 
                            SELECT partid, parents_partid
                            FROM partrelationship r
                            WHERE partid = %s

                            UNION ALL 

                            SELECT r.partid, r.parents_partid
                            FROM partrelationship r, calc 
                            WHERE r.parents_partid = calc.partid 
                        ) 
                        SELECT calc.partid, parents_partid, cfp
                        FROM  calc, cfpval
                        WHERE calc.partid = cfpval.partid
                )
                SELECT pt.partid, parents_partid, assembler, cfp
                FROM part_tree pt, partinfo i
                WHERE pt.partid = i.partid;

                select * from temp;
            """
            cur.execute(sql_, (root_partid, ))

            sql_1 = """
                INSERT INTO hashvals (partid, can_hashing, hash)
                SELECT partid, 'f', ARRAY[digest(cfp::text, 'sha256')]
                FROM temp;

                UPDATE hashvals SET can_hashing = 't'
                    WHERE partid  NOT IN (SELECT r.parents_partid FROM partrelationship r, temp WHERE r.parents_partid = temp.partid);
            """
            cur.execute(sql_1)

            while True: 

                # 終了条件
                cur.execute("SELECT partid FROM hashvals WHERE can_hashing = 'f' LIMIT 1;")
                row = cur.fetchone()

                if not row:
                    break

                sql_2 = """
                    WITH get_can_hashing_part AS (
                        SELECT parents_partid as partid, bool_and(can_hashing) as result, array_agg(hash[1]) AS hash_list
                        FROM hashvals, temp
                        WHERE temp.partid = hashvals.partid
                        Group by parents_partid
                    ),
                    update_hash AS (
                        SELECT r1.partid, array_cat(hash, hash_list) AS new_hash
                        FROM get_can_hashing_part r1, hashvals
                        WHERE r1.partid = hashvals.partid AND result = 't' AND can_hashing = 'f'
                    ),
                    get_xor AS (
                        SELECT partid, xor_sha256(new_hash) AS result 
                        FROM update_hash r2
                    )
                    UPDATE hashvals SET (can_hashing, hash) = ('t', ARRAY[xor_sha256(new_hash)]) 
                    FROM update_hash r2
                    WHERE r2.partid = hashvals.partid;
                """ 
                cur.execute(sql_2)

            sql_5 = """
            WITH valification AS (
                SELECT hashvals.partid, hpt.hash = encode(hashvals.hash[1], 'hex') as result 
                FROM hashvals, hash_parts_tree hpt
                WHERE hashvals.partid = hpt.partid
            )
            SELECT partid FROM valification WHERE result = 'f';
            """

            cur.execute(sql_5)
            data = cur.fetchall()

    finally:
        if conn:
            conn.close()
    
    return data


# ======== MAIN ======== #

if __name__ == '__main__':

    root_partid = 'P0'
    peers = ["postgresA", "postgresB", "postgresC"]

    assembler = w.get_Assebler(root_partid)

    start = time.time()

    result = valification(assembler, peers, root_partid)

    if len(result) == 0:
        print("varification successfully")
    else:
        print(result)
    
    t = time.time() - start
    print("time:", t)

