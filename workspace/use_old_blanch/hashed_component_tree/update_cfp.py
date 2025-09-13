### cfpが変更された時にtarget_partがパスに含まれる全ての部品のhashを更新する
### cfpが変更された時にtotalcfpを更新する

from sqlalchemy import create_engine
import polars as pl
from psycopg2 import sql
import hashlib
from decimal import *
import time

import calculation as c
import SQLexecutor as SQLexe
import write_to_db as w


# ======== DataFrameの表示の仕方 ======== #
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_rows(-1)
pl.Config.set_fmt_str_lengths(n=64)
# ===================================== #



# ====== パス（変更対象部品）の取得 ====== #

def get_path(assembler, target_part):

    engine = create_engine("postgresql://postgres:mysecretpassword@"+assembler+":5432/iroha_default")
    sql_statement ="""
        SELECT r.partid, parents_partid, priority, assembler, hash \
            FROM partrelationship r, partinfo i, hash_parts_tree m \
            WHERE r.partid = i.partid and r.partid = m.partid;
    """

    all_tree = pl.read_database(sql_statement, engine) #木の全体

    parents_partid = all_tree.filter(pl.col("partid") == target_part)["parents_partid"]
    parents_list = [target_part]

    while len(parents_partid) >  0:

        parents_list.append(parents_partid.item())

        if parents_partid.item() == 'null': break # 終了条件
        
        parents_partid = all_tree.filter(pl.col("partid") == parents_partid)["parents_partid"]

    target_tree = all_tree.filter(pl.col("parents_partid").is_in(parents_list)) # 必要なとこだけ抽出

    return parents_list, target_tree



# ====== ハッシュの再計算 ====== #

def recalcu_hash(df, partid):

    child_df = df.filter((pl.col("parents_partid") == partid)).sort(["priority"])
    child_hash_list = child_df.get_column("hash").to_list()

    target_cfp_hash = df.filter(pl.col("partid") == partid)["hash"].item() # 更新するpartidのhashを取得

    new_hash = c.sha256(target_cfp_hash + "".join(child_hash_list)) # 新しいハッシュ

    return new_hash
    
    

# ====== パスのハッシュとtotalcfpを計算 ====== #

def update_hash(assembler, target_part, new_cfp):

    # パスとツリーを取得
    path, tree = get_path(assembler, target_part)

    ## cfpの差分を計算
    new_cfp =  Decimal(new_cfp).quantize(Decimal('0.0001'), ROUND_HALF_UP) # new_cfpの小数点以下4桁まで表示

    sql_statement = sql.SQL("SELECT cfp FROM cfpval where partid = {target_part};" # 既存のCFPを取得
    ).format(
            target_part = sql.Literal(target_part)
    )

    pre_cfp = SQLexe.QUERYexecutor_off(sql_statement, assembler)[0][0]

    cfp_sabun = pre_cfp - new_cfp #差分を計算

    ## hash(new_cfp)を書き込み 

    new_hash = c.sha256(new_cfp)
    tree = tree.with_columns(
        pl.when(pl.col("partid") == target_part)
        .then(pl.lit(c.sha256(new_cfp)))
        .otherwise(pl.col("hash"))
        .alias("hash")
    )


    ## 順番に処理する
    totalcfp = None

    for i in range(len(path)-1):

        target_part = path[i]
        
        assembler_path = w.get_Assebler(target_part) # assemblerを取得
        #print(assembler_path, target_part, new_cfp, totalcfp, cfp_sabun)
        w.update_cfp_to_off(assembler_path, target_part, new_cfp, totalcfp, cfp_sabun) # 新しいtotalCFPを書き込み

        new_hash = recalcu_hash(tree, target_part)

        # new_hashでdataframeをupdate
        tree = tree.with_columns(
            pl.when(pl.col("partid") == target_part)
            .then(pl.lit(new_hash))
            .otherwise(pl.col("hash"))
            .alias("hash")
        )

        new_cfp = None # 一回目だけnew_cfpを更新するため

    result = tree.filter(pl.col("partid").is_in(path))["partid", "hash"]

    # irohaが完成するまで w.upsert_hash_exe(result, "A")

    # Irohaコマンドで書き込み
    part_list, hash_list = w.to_iroha(result)
    SQLexe.IROHA_CMDexe(assembler, part_list, hash_list)

    return 
    


# ======== MAIN ======== #

if __name__ == '__main__':

    target_part = "P5"
    assembler = w.get_Assebler(target_part)

    start = time.time()

    update_hash(assembler, target_part, 0.50)
    
    t = time.time() - start
    print("time:", t)

    