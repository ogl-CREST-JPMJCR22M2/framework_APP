### partidを使って求める

from sqlalchemy import create_engine
import polars as pl
import time
import random
import hashlib

import SQLexecutor as SQLexe
import write_to_db as w


# ======== DataFrameの表示の仕方 ======== #
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_fmt_str_lengths(n=30)
# ===================================== #


# ======== postgresqlに接続して取得する部分 ======== #

# offchainに接続，cfpをget
def get_cfpval(peer):

    engine = create_engine("postgresql://postgres:mysecretpassword@"+peer+":5432/offchaindb")

    sql_statement =" SELECT partid, cfp FROM cfpval;"

    df = pl.read_database(sql_statement, engine)
    
    return df

# offchainから全てのcfpをget，結合
def join_cfpvals(peers):

    df1 = get_cfpval("postgresA")

    for p in peers[1:]:
        df2 = get_cfpval(p)
        df1 = pl.concat([df1, df2])

    return df1

# ======== 部品木の取得 ======== #

def get_part_tree(peer, parents_partid):

    engine = create_engine("postgresql://postgres:mysecretpassword@"+peer+":5432/iroha_default")

    sql_statement ="SELECT partid, parents_partid, priority FROM partrelationship;"

    all_tree = pl.read_database(sql_statement, engine)

    # 部品木を抽出
    def get_childpart(df, parents_partid):

        child_df = df.filter(pl.col("parents_partid") == parents_partid)

        childpart = child_df["partid"].to_list()

        for child in childpart:
            child_df = pl.concat([child_df, get_childpart(df, child)])
        
        return child_df

    df = pl.concat([all_tree.filter(pl.col("partid") == parents_partid), get_childpart(all_tree, parents_partid)])
    
    return df





# ======== 算出部分 ======== #

# ハッシュ化    
def sha256(value: float) -> str:
    return hashlib.sha256(str(value).encode()).hexdigest()

# ハッシュ値を計算する関数
def compute_parent_hashes(df):

    # 末端ノードのハッシュ値を決定
    df = df.with_columns(
        pl.when(~pl.col("partid").is_in(df["parents_partid"]))
        .then(pl.col("cfp").map_elements(sha256, return_dtype=pl.String))
        #.then(pl.col("cfp").cast(pl.String)) #確認用
        .otherwise(None)
        .alias("hash")
    )

    # 子ノードリストを作成
    child_hashes = df.select(["partid", "parents_partid"]).group_by("parents_partid").agg(pl.col("partid").alias("child_parts")).rename({"parents_partid": "partid"})
    
    df = df.join(child_hashes, on="partid", how="left")

    # 子部品の連結
    def get_child_hashes(parts: list[str]) -> str:

        df_ =  df.filter(pl.col("partid").is_in(parts))
        df_ = df_.sort(["priority"])

        hash_values = df_["hash"].to_list()
        clean_hashes = []

        for h in hash_values:
            if h is None:
                return None
            else:
                clean_hashes.append(h)
        
        out = "".join(clean_hashes)
        
        return out

    # ハッシュ値を計算
    while df["hash"].null_count() > 0:

        df = df.with_columns(
            pl.when((pl.col("hash").is_null()) & (pl.col("child_parts").is_not_null()))
            .then(
                pl.concat_str([
                    pl.col("cfp").map_elements(sha256, return_dtype=pl.String),
                    #pl.col("cfp").cast(pl.String),
                    #pl.lit("("),
                    pl.col("child_parts").map_elements(get_child_hashes, return_dtype=pl.String),
                    #pl.lit(")"),
                    ])
                    .map_elements(sha256, return_dtype=pl.String)
            )
            .otherwise(pl.col("hash"))
            .alias("hash")
        )
    
    return df



def make_merkltree(assembler, root_partid):

    peers = ["postgresA", "postgresB", "postgresC"]

    df = get_part_tree(assembler, root_partid) # root_partidがルートの部品木の抽出

    df_h=join_cfpvals(peers) # cfpvalの取得

    """
    #確認用
    df_h = pl.DataFrame(
        {
            "partid" : ["P"+str(i) for i in range(0, 156)],
            "cfp" : [chr(random.randint(0,26) + 97) for i in range(0, 156)]
            #"cfp" : [i for i in range(0, 156)]
        }
    )"""

    df = df.join(df_h, on=["partid"], how="left") # 部品木にcfpvalを結合

    result = compute_parent_hashes(df) # マークル木を計算

    #print(result)
    # irohaが完成するまで w.upsert_hash_exe(result["partid", "hash"], "A")

    # Irohaコマンドで書き込み
    part_list, hash_list = w.to_iroha(result)
    SQLexe.IROHA_CMDexe(assembler, part_list, hash_list)

    return result



# ======== MAIN ======== #

if __name__ == '__main__':

    root_partid = 'P0'
    assembler = w.get_Assebler(root_partid)

    start = time.time()

    df = make_merkltree(assembler, root_partid)
    
    t = time.time() - start
    print("time:", t)

    #totalCFPの算出
    df_total = df.select(pl.col("cfp").sum()).item() # 算出
    w.update_cfp_to_off(assembler, root_partid, totalcfp = df_total) # update
    print("sum:", df_total)
    


# ======= 開発用 ======= #

# decimal -> str    
def to_string(value: float) -> str:
    return str(value)