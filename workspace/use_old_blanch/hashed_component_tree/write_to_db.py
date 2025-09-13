### partidを使って求める

from sqlalchemy import create_engine
import polars as pl
from psycopg2 import connect, sql
from psycopg2.extras import execute_values
import SQLexecutor as SQLexe



### irohaに渡すためのデータ加工

def to_iroha(df):

    part_list = df["partid"].to_list()
    hash_list = df["hash"].to_list()

    return part_list, hash_list


### offchaindbのcfpの更新

def update_cfp_to_off(assembler, target_part, cfp = None, totalcfp = None, sabun = None):

    if totalcfp == None and cfp == None and sabun != None :

        sql_statement = sql.SQL(
            """
            UPDATE cfpval SET totalcfp = totalcfp - {sabun} WHERE partid = {target_part};
            """
        ).format(
            target_part = sql.Literal(target_part),
            sabun = sql.Literal(sabun)
        )
        SQLexe.COMMANDexecutor_off(sql_statement, assembler)

    elif totalcfp == None and cfp != None and sabun != None :

        sql_statement = sql.SQL(
            """
            UPDATE cfpval set (totalcfp, cfp) = (totalcfp - {sabun}, {cfp}) where partid = {target_part};
            """
        ).format(
            target_part = sql.Literal(target_part),
            sabun = sql.Literal(sabun),
            cfp = sql.Literal(cfp)
        )

        SQLexe.COMMANDexecutor_off(sql_statement, assembler)
    
    elif totalcfp != None and cfp == None and sabun == None :

        sql_statement = sql.SQL(
            """
            UPDATE cfpval set totalcfp = {totalcfp} where partid = {target_part};
            """
        ).format(
            target_part = sql.Literal(target_part),
            totalcfp = sql.Literal(totalcfp)
        )

        SQLexe.COMMANDexecutor_off(sql_statement, assembler)


### ASSEMBLERを取得

def get_Assebler(target_part):

    sql_statement = sql.SQL(
            """
            SELECT * FROM partinfo WHERE partid = {target_part};
            """
        ).format(
            target_part = sql.Literal(target_part)
        )
        
    return SQLexe.QUERYexecutor_on(sql_statement, "postgresA")[0][1]


### ハッシュ値を取得

def get_hash(target_part):

    sql_statement = sql.SQL(
            """
            SELECT hash FROM hash_parts_tree WHERE partid = {target_part};
            """
        ).format(
            target_part = sql.Literal(target_part)
        )
        
    return SQLexe.QUERYexecutor_on(sql_statement, "postgresA")[0][1]


### まとめてhashをUPSERT

def upsert_hash_exe(df, assembler):

    upsert_sql = """
        INSERT INTO hash_parts_tree (partid, hash)
        VALUES %s
        ON CONFLICT (partid) DO UPDATE SET hash = EXCLUDED.hash;
    """

    # DB接続
    conn = connect(
        dbname = "iroha_default", 
        user = "postgres", 
        password = "mysecretpassword",
        host = assembler,
        port = 5432
    )

    with conn.cursor() as cur:
        data = df.select(["partid", "hash"]).rows()
        execute_values(cur, upsert_sql, data)

    conn.commit()
    conn.close()



### 一回だけhashをUPSERT

def upsert_hash(partid, hash_val, assembler):

    upsert_sql = sql.SQL( """
        INSERT INTO hash_parts_tree (partid, hash)
        VALUES ({partid}, {hash_val})
        ON CONFLICT (partid) DO UPDATE SET hash = {hash_val};
    """
    ).format(
            partid = sql.Literal(partid),
            hash_val = sql.Literal(hash_val)
    )

    SQLexe.COMMANDexecutor_on(upsert_sql, assembler)


### まとめてoffchaindbごとにcfpvalをUPSERT

def upsert_totalcfp_exe(df, assembler):

    upsert_sql = """
        UPDATE cfpval AS t
        SET
            partid = v.partid,
            totalcfp = v.totalcfp
        FROM (VALUES %s)
        AS v(partid, totalcfp)
        WHERE v.partid = t.partid;
    """

    # DB接続
    conn = connect(
        dbname = "offchaindb", 
        user = "postgres", 
        password = "mysecretpassword",
        host = assembler,
        port = 5432
    )

    with conn.cursor() as cur:
        data = df.select(["partid", "totalcfp"]).rows()
        execute_values(cur, upsert_sql, data)

    conn.commit()
    conn.close()
    

# ======== MAIN ======== #

if __name__ == '__main__':

    print(get_Assebler('P0'))
