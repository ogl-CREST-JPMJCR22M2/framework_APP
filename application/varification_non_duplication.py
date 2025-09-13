### partidを使って求める

import time
import hashlib
from decimal import *
from typing import Optional
from psycopg2 import connect, sql
from psycopg2._psycopg import connection, cursor
from psycopg2.extras import execute_values

import SQLexecutor as SQLexe
import write_to_db as w


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
                    cfp DECIMAL, 
                    PRIMARY KEY (partid)
                );

                CREATE TEMP TABLE hashvals(
                    partid CHARACTER varying(288),
                    can_hashing boolean,
                    hash bytea[],
                    PRIMARY KEY (partid)
                );

                CREATE TEMP TABLE path(
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    hash bytea,
                    hash_on bytea,
                    hash_kensho bytea,
                    PRIMARY KEY (partid)
                );

                CREATE INDEX idx_temp ON temp(partid);
                CREATE INDEX idx_hash ON hashvals(partid);
                CREATE INDEX idx_hash_temp ON hashvals(hash);
                CREATE INDEX idx_path ON path(partid);
                
            """)
            
            ## cfpの算出
            # offchain-dbからcfpの算出
            cfp_import = " UNION ALL \n".join(["SELECT * FROM dblink('host="+ p +" port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 'SELECT partid, cfp FROM cfpval') AS t1(partid CHARACTER varying(288), cfp DECIMAL)" 
            for p in peers ]) 
        
            sql_ = f"""
                INSERT INTO temp (partid, parents_partid, cfp) 
                WITH cfpval AS (
                    {cfp_import}
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
                        SELECT * FROM calc
                )
                SELECT pt.partid, parents_partid, cfp
                FROM part_tree pt, cfpval
                WHERE pt.partid = cfpval.partid;

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

            # 検証
            
            sql_3 = """
                INSERT INTO path (partid, hash, hash_on)
                WITH va AS (
                    SELECT hpt.partid, hashvals.hash[1]::bytea AS hash, decode(hpt.hash, 'hex') AS hash_on, hpt.hash = encode(hashvals.hash[1], 'hex') as result
                    FROM hashvals, hash_parts_tree hpt
                    WHERE hashvals.partid = hpt.partid
                )
                SELECT partid, hash, hash_on
                FROM va WHERE result = False;

                SELECT count(partid) FROM path;
            """
            cur.execute(sql_3)
            row = cur.fetchone()

            if row[0] == 0 : return True # 出力がない = 検証成功


            ## 特定処理続行
            cur.execute("""
                UPDATE path SET parents_partid = temp.parents_partid
                FROM temp
                WHERE temp.partid = path.partid;
                    
                SELECT partid FROM path WHERE partid NOT IN (SELECT parents_partid FROM path);
            """)
            target = cur.fetchall()

            kaizan_kamo = set()
            
            while True:
                ## 検証失敗のため特定処理へ
                # 初期値点ごとで繰り返し
                for n in target:

                    now_searching = n[0]
                    kaizan_kamo.add(now_searching)

                    cur.execute(f"""
                        WITH target_hash AS (
                            SELECT xor_sha256(ARRAY[hash, hash_on]) AS xor_hash 
                            FROM path 
                            WHERE partid = '{now_searching}'
                        ),
                        do_xor AS (
                            WITH RECURSIVE xor_leaf(partid, parents_partid) AS (
                                SELECT partid, parents_partid, hash_on AS done_xor_hash
                                FROM path
                                WHERE partid = '{now_searching}'

                                UNION

                                SELECT p.partid, p.parents_partid, xor_sha256(ARRAY[hash, xor_hash]) AS done_xor_hash
                                FROM path p, target_hash, xor_leaf
                                WHERE p.partid = xor_leaf.parents_partid
                            )
                            SELECT partid, done_xor_hash
                            FROM xor_leaf
                        )
                        UPDATE path SET hash = done_xor_hash
                        FROM do_xor dx
                        WHERE dx.partid = path.partid;
                    """)
                
                # 検証
                cur.execute("""
                    WITH checking AS (
                        SELECT partid, parents_partid, hash = hash_on AS result
                        FROM path
                    ),
                    false_list AS (
                        SELECT partid, parents_partid FROM checking WHERE result = False
                    )
                    SELECT partid FROM false_list WHERE partid NOT IN (SELECT parents_partid FROM false_list);
                """)
                target = cur.fetchall()  

                if len(target) == 0 : break # 出力がない = 検証終了

    finally:
        if conn:
            conn.close()
    
    return kaizan_kamo

# ======== MAIN ======== #

if __name__ == '__main__':

    root_partid = 'P0'
    peers = ["postgresA", "postgresB", "postgresC"]

    assembler = w.get_Assebler(root_partid)

    start = time.time()

    result = valification(assembler, peers, root_partid)

    if result == True:
        print("varification successfully")
    else:
        print(result)
        print(len(result))
    
    t = time.time() - start
    print("time:", t)

