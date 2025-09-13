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
                CREATE TEMP TABLE target_tree (
                    partid CHARACTER varying(288),
                    parents_partid CHARACTER varying(288),
                    qty int,
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
                CREATE INDEX idx_hash_val ON hashvals(hash);
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
            co2_import = " UNION ALL \n".join(["SELECT * FROM dblink('host="+ p +" port=5432 dbname=offchaindb user=postgres password=mysecretpassword', 'SELECT partid, cfp FROM cfpval') AS t1(partid CHARACTER varying(288), cfp DECIMAL)" 
            for p in peers ]) 
        

            sql_2 = f"""
                -- cfp算出
                INSERT INTO calc_cfp (partid, cfp, hash_cfp) 
                WITH cfpvals AS (
                    {co2_import}
                )
                SELECT DISTINCT cv.partid, cfp, digest(cfp::text, 'sha256') AS hash_cfp
                FROM cfpvals cv, target_tree tt
                WHERE cv.partid = tt.partid;

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

            # 検証

            sql_5 = f"""
                SELECT hpt.hash = encode(hashvals.hash, 'hex') AS result
                    FROM hashvals, hash_parts_tree hpt
                    WHERE hashvals.partid = hpt.partid AND hpt.partid = %s;
            """
            cur.execute(sql_5, (root_partid, ))
            row = cur.fetchone()
            

            if row[0] == True : return True # 出力がない = 検証成功

            ## 特定処理続行

            cur.execute("""
                CREATE TEMP TABLE potential_kaizan(
                    child_partid CHARACTER varying(288),
                    partid CHARACTER varying(288),
                    duplication boolean,
                    hash bytea,
                    hash_c bytea,
                    hash_on bytea,
                    UNIQUE (partid, child_partid)
                );

                CREATE INDEX IF NOT EXISTS i_potkz_partid_child ON potential_kaizan(partid, child_partid);

                INSERT INTO potential_kaizan (partid, child_partid, duplication, hash, hash_c, hash_on)
                    SELECT DISTINCT
                        h.parents_partid,
                        h.partid, 
                        h.duplication,
                        hp.hash AS hash,
                        h.hash AS hash_c,
                         decode(hpt.hash, 'hex') AS hash_on
                    FROM hashvals h
                    JOIN hash_parts_tree hpt ON h.partid = hpt.partid
                    LEFT JOIN hashvals hp ON h.parents_partid = hp.partid
                    WHERE hpt.hash <> encode(h.hash, 'hex');

                SELECT child_partid FROM potential_kaizan WHERE child_partid NOT IN (SELECT partid FROM potential_kaizan);
            """)
            target = cur.fetchall()

            kaizan_kamo = set()

            cur.execute("SELECT 1 FROM potential_kaizan WHERE duplication = True;")
            duplication = cur.fetchall()

            if len(duplication) >0 :
            
                while True:
                    ## 検証失敗のため特定処理へ
                    # 初期値点ごとで繰り返し
                    for n in target:

                        now_searching = n[0]
                        kaizan_kamo.add(now_searching)

                        cur.execute(f"""
                            WITH do_xor AS (
                                WITH RECURSIVE do_xor_(partid, child_partid, done_xor_hash) AS (
                                    SELECT partid, child_partid,
                                        CASE duplication
                                            WHEN True THEN xor_sha256(ARRAY[
                                                    p.hash, 
                                                    digest( p.hash_c::text || p.partid::text, 'sha256'),
                                                    (SELECT decode(hash, 'hex') FROM hash_parts_tree WHERE partid = '{now_searching}')
                                                ])
                                            ELSE xor_sha256(ARRAY[p.hash, p.hash_c, 
                                                (SELECT decode(hash, 'hex') FROM hash_parts_tree WHERE partid = '{now_searching}')
                                            ]) 
                                        END AS done_xor_hash,
                                        p.hash
                                    FROM potential_kaizan p
                                    WHERE child_partid = '{now_searching}'

                                    UNION

                                    SELECT p.partid, p.child_partid, 
                                        CASE duplication
                                            WHEN True THEN xor_sha256(ARRAY[
                                                    p.hash, 
                                                    digest( p.hash_c::text || p.partid::text, 'sha256'),
                                                    digest( x.done_xor_hash::text || p.partid::text, 'sha256')
                                                ])
                                            ELSE xor_sha256(ARRAY[ p.hash, hash_c, x.done_xor_hash ])
                                    END AS done_xor_hash,
                                    p.hash
                                    FROM potential_kaizan p, do_xor_ x
                                    WHERE p.child_partid = x.partid
                                )
                                SELECT * FROM do_xor_
                            ), 
                            do_synchro AS (
                                SELECT partid,
                                    CASE 
                                        WHEN count(partid) > 1 THEN xor_sha256(array_agg(done_xor_hash) || any_value(hash) )
                                        ELSE any_value(done_xor_hash)
                                    END AS done_synchro
                                FROM do_xor
                                GROUP BY partid
                            )
                            UPDATE potential_kaizan p
                                SET (hash, hash_c) = (d.hash, d.hash_c )
                            FROM (
                                SELECT p.partid, p.child_partid,
                                ds1.done_synchro AS hash,
                                ds2.done_synchro AS hash_c
                                FROM potential_kaizan p
                                LEFT JOIN do_synchro ds1 ON ds1.partid = p.partid
                                LEFT JOIN do_synchro ds2 ON ds2.partid = p.child_partid
                            ) AS d
                            WHERE p.partid = d.partid AND p.child_partid = d.child_partid;
                        """)
                    
                    # 検証

                    cur.execute("""
                        WITH false_list AS (
                            SELECT p.partid, child_partid, p.hash
                            FROM potential_kaizan p, hash_parts_tree hpt
                            WHERE hpt.hash <> encode(p.hash, 'hex') AND hpt.partid = p.partid
                        )
                        SELECT partid 
                            FROM false_list 
                            WHERE partid IN (SELECT child_partid FROM false_list);
                    """)
                    target = cur.fetchall()  

                    if len(target) == 0 : break # 出力がない = 検証終了

            else:

                while True:
                    ## 検証失敗のため特定処理へ
                    # 初期値点ごとで繰り返し
                    for n in target:

                        now_searching = n[0]
                        kaizan_kamo.add(now_searching)

                        cur.execute(f"""
                            WITH target_hash AS (
                                SELECT 
                                xor_sha256(ARRAY[ hash_c, hash_on])AS xor_hash 
                                FROM potential_kaizan p
                                WHERE p.child_partid = '{now_searching}'
                            ),
                            do_xor AS (
                                WITH RECURSIVE do_xor(partid, child_partid) AS (
                                    SELECT partid, child_partid, 
                                        xor_sha256(ARRAY[hash, xor_hash]) AS done_xor_hash
                                    FROM potential_kaizan, target_hash
                                    WHERE child_partid = '{now_searching}'

                                    UNION

                                    SELECT p.partid, p.child_partid, 
                                        xor_sha256(ARRAY[hash, xor_hash]) AS done_xor_hash
                                    FROM potential_kaizan p, target_hash, do_xor
                                    WHERE p.child_partid = do_xor.partid
                                )
                                SELECT partid, done_xor_hash
                                FROM do_xor
                            )
                            UPDATE potential_kaizan SET hash = done_xor_hash
                            FROM do_xor dx
                            WHERE dx.partid = potential_kaizan.partid;
                        """)
                    
                    # 検証
                    cur.execute("""
                        WITH false_list AS (
                            SELECT partid, child_partid
                            FROM potential_kaizan
                            WHERE hash_on <> hash
                        )
                        SELECT partid 
                        FROM false_list 
                        WHERE partid IN (SELECT child_partid FROM false_list);
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
        print("varification faild")
    
    t = time.time() - start
    print("time:", t)

