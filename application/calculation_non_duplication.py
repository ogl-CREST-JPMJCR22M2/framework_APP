### partidを使って求める

import time
import hashlib
from decimal import *
from typing import Optional
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
from psycopg2._psycopg import connection, cursor
from collections import defaultdict

import SQLexecutor as SQLexe
import write_to_db as w


def hash_part_tree(peer, peers, root_partid):

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
            """)
            
            ## cfpの算出
            # offchain-dbからcfpの算出
            co2_import = " UNION ALL \n".join(["SELECT * FROM dblink('host="+ p +" port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 'SELECT partid, co2 FROM cfpval') AS t1(partid CHARACTER varying(288), co2 DECIMAL)" 
            for p in peers ]) 
        
            sql_ = f"""
                INSERT INTO temp (partid, parents_partid,  assembler, cfp, co2) 
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
                        SELECT calc.partid, parents_partid,  co2 
                        FROM  calc, cfpval
                        WHERE calc.partid = cfpval.partid
                ), 
                getcfp AS (
                    WITH RECURSIVE subtree_sum(root_id, current_id, co2) AS 
                        ( 
                            SELECT 
                            partid AS root_id, partid AS current_id, co2
                            FROM part_tree

                            UNION ALL 

                            SELECT ss.root_id, pt.partid AS current_id, pt.co2
                            FROM subtree_sum ss, part_tree pt
                            WHERE pt.parents_partid = ss.current_id
                        ) 
                        SELECT 
                        root_id AS partid, SUM(co2) AS cfp
                        FROM subtree_sum
                        GROUP BY root_id
                )
                SELECT pt.partid, parents_partid, assembler, cfp, co2
                FROM part_tree pt, getcfp gt, partinfo i
                WHERE pt.partid = gt.partid AND pt.partid = i.partid;
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
                    )
                    UPDATE hashvals SET (can_hashing, hash) = ('t', ARRAY[xor_sha256(new_hash)]) 
                    FROM update_hash r2
                    WHERE r2.partid = hashvals.partid;
                """ 
                cur.execute(sql_2)
                
            sql_5 = "SELECT temp.partid, assembler, cfp, encode(hash[1], 'hex') as hashval FROM temp, hashvals WHERE temp.partid = hashvals.partid;"
            cur.execute(sql_5)
            data = cur.fetchall()

    finally:
        if conn:
            conn.close()
    
    return data


def make_merkltree(assembler, root_partid):

    peers = ["postgresA", "postgresB", "postgresC"]

    start = time.time()
    ## postgres処理
    result = hash_part_tree(assembler, peers, root_partid)

    #print("ツリー構築",time.time()-start)
    start = time.time()

    # polarsに変換
    part_list = []
    hash_list = []

    # assemblerごとの2次元リスト (insert_val)
    insert_val_dict = defaultdict(list)

    for partid, assembler, cfp, hashval in result:
        part_list.append(partid)
        hash_list.append(hashval)
        insert_val_dict[assembler].append((partid, cfp))

    # assemblerの辞書のkey
    assembler_unique = list(insert_val_dict.keys())

    #print("データの抽出",time.time()-start)
    start = time.time()

    # Irohaコマンドで書き込み
    SQLexe.IROHA_CMDexe(assembler, part_list, hash_list)

    #print("iroha実行",time.time()-start)
    start = time.time()

    # offchain-dbへの書き込み
    for key in assembler_unique:

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
            host = key,
            port = 5432
        )

        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, insert_val_dict[key])

        conn.commit()
        conn.close()

    #print("offchainへwrite",time.time()-start)


# ======== MAIN ======== #

if __name__ == '__main__':

    root_partid = 'P0'
    assembler = w.get_Assebler(root_partid)

    start = time.time()

    peers = ["postgresA", "postgresB", "postgresC"]
    #result = hash_part_tree(assembler, peers, root_partid)
    #print(result)
    make_merkltree(assembler, root_partid)
    
    t = time.time() - start
    print("time:", t)
