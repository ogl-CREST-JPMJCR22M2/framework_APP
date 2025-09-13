### 1000回の更新トランザクションのうち，更新対象部品が更新される割合をPareto分布に従って決定

import numpy as np
import random
import polars as pl
import time
from concurrent.futures import ThreadPoolExecutor
import threading

import update as u
import write_to_db as w


# Set seed for reproducibility
np.random.seed(42)

num_total_parts = 19531
num_transactions = 1953
update_percent = [1, 5, 10, 15, 20] # %で
percent = update_percent[3]


# ===== 更新対象部品を選ぶ =======
parts = [f"P{i}" for i in range(num_total_parts)]

# ランダムに選択
sampled_parts = random.sample(parts, int(num_total_parts * percent * 0.01))
num_part = len(sampled_parts)

# Pareto distribution: skewed access to partitions
alpha = 1.5
raw_pareto = np.random.pareto(alpha, num_transactions)
pareto_transactions = np.floor(num_part * raw_pareto / (raw_pareto.max() + 1e-8)).astype(int)

# ===== インデックスから部品IDに変換 =====
mapped_parts_p = [sampled_parts[i] for i in pareto_transactions]


# ===== ログ設定 =====
log_lock = threading.Lock()
log_path = "./result15.log"

def log(msg):
    with log_lock:
        with open(log_path, "a") as f:
            print(msg, file=f, flush=True)

# ===== トランザクション実行関数 =====
def exec_transaction(target_part):
    try:
        new_cfp = random.random()
        assembler = w.get_Assebler(target_part)

        start = time.time()
        u.make_merkltree(assembler, target_part, new_cfp)
        elapsed = time.time() - start

        log(elapsed)

    except Exception as e:
        log(f"Error with {target_part}: {e}")

# ===== 実行 =====
total_start = time.time()

with ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(exec_transaction, mapped_parts_p)

total_elapsed = time.time() - total_start

print("total time: ", total_elapsed)
print("average time:", total_elapsed / num_transactions)
