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
                CREATE TEMP TABLE target_tree (
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    qty NUMERIC(100,0),
                    UNIQUE (partid, parents_partid)
                );
                
                CREATE TEMP TABLE calc_cfp (
                    partid CHARACTER varying(288),
                    cfp DECIMAL, 
                    hash_cfp bytea,
                    PRIMARY KEY (partid)
                );

                CREATE TEMP TABLE hashvals(
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    can_hashing boolean,
                    duplication boolean,
                    hash bytea,
                    UNIQUE (partid, parents_partid)
                );

                CREATE INDEX idx_tree ON target_tree(partid);
                CREATE INDEX idx_cfp ON calc_cfp(partid);
                CREATE INDEX idx_hash ON hashvals(partid);
            """)
            
            # 部品木の抽出
            sql_1 = f"""
                INSERT INTO target_tree (partid, parents_partid, qty) 
                    WITH RECURSIVE get_tree(partid, parents_partid) AS 
                        ( 
                            SELECT partid, parents_partid, qty
                            FROM partrelationship
                            WHERE partid = %s

                            UNION

                            SELECT r.partid, r.parents_partid, r.qty
                            FROM partrelationship r, get_tree gt
                            WHERE r.parents_partid = gt.partid 
                        )
                        SELECT gt.partid, gt.parents_partid, qty
                        FROM get_tree gt;
                """
            cur.execute(sql_1, (root_partid, ))

            ## cfpの算出
            # offchain-dbからcfpの算出
            co2_import = " UNION ALL \n".join(["SELECT * FROM dblink('host="+ p +" port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 'SELECT partid, co2 FROM cfpval') AS t1(partid CHARACTER varying(288), co2 DECIMAL)" 
            for p in peers ]) 
        

            sql_2 = f"""
                -- cfp算出
                INSERT INTO calc_cfp (partid, cfp, hash_cfp) 
                WITH co2vals AS (
                    {co2_import}
                ),
                cfpvals AS(               
                    WITH RECURSIVE calc_qty(partid, root, quantity) AS (
                        SELECT DISTINCT
                            tt.partid,
                            tt.partid AS root,
                            1:: NUMERIC(100,0) AS quantity
                        FROM target_tree tt

                        UNION ALL

                        SELECT
                            tt.partid,          -- 子部品
                            cq.root,           -- スタート部品は固定
                            (cq.quantity * tt.qty):: NUMERIC(100,0) -- 親の個数 * 使用数（qty）
                        FROM calc_qty cq
                        JOIN target_tree tt ON tt.parents_partid = cq.partid
                    )
                    SELECT
                        cq.root AS partid,
                        ROUND(SUM(c.co2 * cq.quantity), 4) AS cfp
                    FROM calc_qty cq
                    JOIN co2vals c ON cq.partid = c.partid
                    GROUP BY cq.root
                    ORDER BY cq.root
                )
                SELECT partid, cfp, digest(cfp::text, 'sha256') AS hash_cfp
                FROM cfpvals;

            """
            cur.execute(sql_2)

            # 単品部品の処理
            sql_3 = f"""
                INSERT INTO hashvals (partid, parents_partid, can_hashing, duplication, hash)
                    WITH check_duplication AS (
                        SELECT partid, COUNT(partid) > 1 AS duplication
                        FROM target_tree
                        GROUP BY partid
                    )
                    SELECT 
                        tt.partid, tt.parents_partid,
                        -- ハッシュ化可能か
                        CASE 
                            WHEN EXISTS ( SELECT 1 FROM target_tree tt WHERE tt.parents_partid = calc_cfp.partid ) THEN False
                            ELSE True
                        END AS can_hashing,
                        -- 重複チェック
                        duplication,
                        hash_cfp AS hash
                    FROM calc_cfp, target_tree tt, check_duplication cd
                    WHERE calc_cfp.partid = tt.partid AND tt.partid = cd.partid;
            """
            cur.execute(sql_3)
            
            while True: 

                # 終了条件
                cur.execute("SELECT partid FROM hashvals WHERE can_hashing = False LIMIT 1;")
                row = cur.fetchone()

                if not row:
                    break

                sql_4 = """
                    WITH get_can_hashing AS (
                        SELECT 
                            parents_partid, 
                            bool_and(can_hashing)
                        FROM hashvals
                        GROUP BY parents_partid 
                        HAVING bool_and(can_hashing) = True
                    ), 

                    check_dup AS (
                        SELECT partid AS partid_org, gch.parents_partid AS partid,
                            CASE duplication 
                                WHEN True THEN digest( hash::text || gch.parents_partid::text, 'sha256')
                                ELSE hash
                            END AS hash_under_calc
                        FROM get_can_hashing gch, hashvals h
                        WHERE gch.parents_partid = h.parents_partid
                    ),

                    hashing AS (
                        SELECT 
                            partid,
                            array_agg(hash_under_calc) AS hash_list
                        FROM check_dup
                        GROUP BY partid 
                    )

                    UPDATE hashvals SET (can_hashing, hash) = (True, xor_sha256(hash_list || hash)) 
                    FROM hashing h
                    WHERE h.partid = hashvals.partid AND can_hashing = False;
                """ 
                cur.execute(sql_4)
                
            cur.execute( """
                SELECT DISTINCT calc_cfp.partid, assembler, cfp, encode(hash, 'hex') AS hashval 
                FROM calc_cfp, hashvals, partinfo pi 
                WHERE calc_cfp.partid = hashvals.partid AND calc_cfp.partid = pi.partid;
                """)
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
