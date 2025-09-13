### child_partidを使って求める

import SQLexecutor as SQLexe
from psycopg2 import sql
from sqlalchemy import create_engine
import polars as pl
import time
import random
import hashlib

# ======== DataFrameの表示の仕方 ======== #
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_fmt_str_lengths(n=30)
# ===================================== #


# ======== postgresqlに接続して取得する部分 ======== #

# offchainに接続，cfpをget
def get_cfpval(peer):

    engine = create_engine("postgresql://postgres:mysecretpassword@postgres"+peer+":5432/offchaindb")

    sql_statement =" SELECT partid, cfp FROM cfpval;"

    df = pl.read_database(sql_statement, engine)
    
    return df

# offchainから全てのcfpをget，結合
def join_cfpvals(peers):

    #peer0 = peers[0]
    df1 = get_cfpval("A")

    for p in peers[1:]:
        df2 = get_cfpval(p)
        df1 = pl.concat([df1, df2])

    return df1


# ======== postgresqlに接続して書き込む部分 ======== #

def write_cfpval(df, peer):

    engine = create_engine("postgresql://postgres:mysecretpassword@postgres"+peer+":5432/offchaindb")

    sql_statement =" SELECT partid, cfp FROM cfpval;"

    df = pl.read_database(sql_statement, engine)
    
    return df


# ======== 部品木の取得 ======== #

def get_tree_2(peer, target_partid):

    engine = create_engine("postgresql://postgres:mysecretpassword@postgres"+peer+":5432/iroha_default")

    sql_statement ="SELECT partid, child_partid FROM partrelationship;"

    all_tree = pl.read_database(sql_statement, engine)

    # 部品木を抽出
    def get_target_tree(df, target_partid):

        target_df = df.filter(pl.col("partid") == target_partid)
        queue = target_df["child_partid"].to_list()[0]

        while len(queue) > 0:

            sento = queue.pop()

            sento_df = df.filter(pl.col("partid") == sento)

            target_df = pl.concat([target_df, sento_df])

            sento_queue = sento_df["child_partid"].to_list()[0]

            if sento_queue is None:
                continue

            queue.extend(sento_queue)

        return target_df
    
    result = get_target_tree(all_tree, target_partid)

    return result


# ======== 算出部分 ======== #

# ハッシュ化    
def sha256(value: float) -> str:
    return hashlib.sha256(str(value).encode()).hexdigest()

# decimal -> str    
def to_string(value: float) -> str:
    return str(value)

# ハッシュ値を計算する関数
def compute_parent_hashes(df):

    # 末端ノードのハッシュ値を決定
    df = df.with_columns(
        pl.when(pl.col("child_partid").is_null())
        .then(pl.col("cfp").map_elements(sha256, return_dtype=pl.String))
        #.then(pl.col("cfp").map_elements(to_string)) #確認用
        .otherwise(None)
        .alias("hash_value")
    )
    
    def get_child_hashes(parts: list[str]) -> str:
        hash_values = df.filter(pl.col("partid").is_in(parts))["hash_value"].to_list()
        clean_hashes = []

        for h in hash_values:
            if h is None:
                return None
            else:
                clean_hashes.append(h)
        
        out = "".join(clean_hashes)
        
        return out

    # ハッシュ値を計算
    while df["hash_value"].null_count() > 0:

        df = df.with_columns(
            pl.when((pl.col("hash_value").is_null()) & (pl.col("child_partid").is_not_null()))
            .then(
                pl.concat_str([
                    pl.col("cfp").map_elements(sha256, return_dtype=pl.String),
                    #pl.col("cfp"),
                    pl.lit("("),
                    pl.col("child_partid").map_elements(get_child_hashes, return_dtype=pl.String),
                    pl.lit(")"),
                    ]).map_elements(sha256, return_dtype=pl.String)
            )
            .otherwise(pl.col("hash_value"))
            .alias("hash_value")
        )
    
    print(df)    
            
    return df





# ======== MAIN ======== #

if __name__ == '__main__':

    peer = "A"
    peers = ["A", "B", "C"]
    partid = 'P0'

    start = time.time()

    df = get_tree_2(peer, partid)
    
    df_h=join_cfpvals(peers)
    df = df.join(df_h, on=["partid"], how="left")
    
    result = compute_parent_hashes(df)

    t = time.time() - start
    print(t)

    # totalCFPの算出
    df_total = df.select(pl.col("cfp").sum()).item()
    print(df_total)
    

    

# ============== 開発過程 ============== #

#child_partidを生成し，update文を生成するプログラム

def generate_update_sql(df: pl.DataFrame) -> list[str]:
    sql_list = []

    for row in df.iter_rows(named=True):
        partid = row["partid"]
        children = row["child_parts"]

        if children is None:
            continue

        children_sql_array = "{" + ", ".join(f'"{c}"' for c in children) + "}"

        sql = f"UPDATE partrelationship SET child_partid = '{children_sql_array}' WHERE partid = '{partid}';"
        sql_list.append(sql)

    for s in sql_list:
        print(s)

    return sql_list

#　priorityの設定
def generate_update_sql_p(com_num):

    sql_list = []
    p = 0

    for n in range(com_num):
        partid = "P"+str(n)

        if n == 0:
            sql = f"UPDATE partrelationship SET priority = '{p}' WHERE partid = '{partid}';"
            P = 0
        else:
            sql = f"UPDATE partrelationship SET priority = '{p}' WHERE partid = '{partid}';"
            
            p += 1

            if p == 5:
                p = 0
        
        sql_list.append(sql)

    for s in sql_list:
        print(s)

    return sql_list
