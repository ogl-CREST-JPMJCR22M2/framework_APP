import csv
import random
import hashlib
import os
from collections import deque

# 定数の定義
NUM_part = 30000  # 部品の種類数
MIN_CHILDREN = 0  # 子供の最小数
MAX_CHILDREN = 5  # 子供の最大数


def new_generate_part_tree(num_part, min_children, max_children):
    from collections import defaultdict

    part = [('P0', 'null', 1)]  # 初期ルートノード
    queue = [('P0', set(['P0']))]  # (parent_id, ancestor_set)

    used_edges = set()
    parent_map = defaultdict(set) 
    next_part_id = 1

    qty_weights = [1, 2, 4]
    qty_probs = [1, 0, 0]  # 確率: 8:1:1

    total_nodes = 1  # P0含む

    while total_nodes < num_part and queue:
        parent_id, ancestors = queue.pop(0)
        num_children = random.randint(min_children, max_children)

        if num_children < 2:
            continue

        for _ in range(num_children):
            if total_nodes >= num_part:
                break

            # 既存の部品を再利用する確率あり（枝の再利用）
            reuse_candidates = []
            
            for i in range(5):
                pid = random.choice(part)[0]
                if pid not in ancestors and (pid, parent_id) not in used_edges and pid != parent_id:
                    reuse_candidates.append(pid)

            if random.random() < 0 and reuse_candidates and total_nodes > NUM_part/2: # 一度登場した部品を再度選ぶ
                child_id = random.choice(reuse_candidates)
                
            else: # 新しい部品を選ぶ
                child_id = f'P{next_part_id}'
                queue.append((child_id, ancestors | {child_id}))
                next_part_id += 1

            if (child_id, parent_id) not in used_edges:  # (child_id, parent_id)が重複しない場合

                used_edges.add((child_id, parent_id))
                qty = random.choices(qty_weights, weights=qty_probs)[0]
                part.append((child_id, parent_id, qty))
                parent_map[child_id].add(parent_id)

                total_nodes += qty

    return part, total_nodes


def count_total_parts(part_tree):
    from collections import defaultdict

    # 部品ごとの子部品リスト {親: [(子, 数量), ...]}
    children_map = defaultdict(list)
    # 部品の出現回数（部品が何回親に登場したか）{部品: 出現数}
    part_qty_count = defaultdict(int)

    for child, parent, qty in part_tree:
        children_map[parent].append((child, qty))
        part_qty_count[child] += 1

    # メモ化用：各部品が構成する全子部品数（再帰的な合計）
    memo = {}

    def count_recursive(part_id):
        if part_id in memo:
            return memo[part_id]

        total = 1  # 自分自身も1と数える（必要なければ total = 0 に）
        for child, qty in children_map.get(part_id, []):
            total += qty * count_recursive(child)

        memo[part_id] = total
        return total

    # 「P0」から全体をカウント
    grand_total = count_recursive('P0')
    #print(f"\n【総部品数（再帰的にカウント）】: {grand_total}")
    return grand_total


# partinfo専用のCSVファイル出力コード
def wirte_csv(part_tree, filename, filename2, filename3, filenameB, filenameC, num):

    partinfo = [['partid', 'assembler']]
    partrelationship =[ ['partid', 'parents_partid', 'qty']]
    hash_part_tree = [['partid', 'hash']]
    data_B = [['partid', 'cfp', 'co2']]
    data_C = [['partid', 'cfp', 'co2']]
    assemblers = ['postgresB', 'postgresC']

    not_dup_partid = set()

    for i in part_tree:
        partrelationship.append([i[0], i[1], i[2]])
        not_dup_partid.add(i[0])

    not_dup_partid = list(not_dup_partid)
    total_num = count_total_parts(part_tree)

    for p in not_dup_partid:

        assembler = random.choice(assemblers)
        cfpval = round(random.uniform(0.1, 1.0), 4)

        if p == 'P0' : assembler = 'postgresA'
        elif assembler == 'postgresB': data_B.append([p, cfpval, cfpval])
        else : data_C.append([p, cfpval, cfpval])

        partinfo.append([p, assembler])
        hash_part_tree.append([p, 'null'])


    # directly作成
    drectly_path = './'+ str(NUM_part) + '/' + str(num)  + '/'
    os.makedirs(drectly_path, exist_ok=True)

    with open(drectly_path + filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(partrelationship)

    with open(drectly_path + filename2, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(partinfo)

    with open(drectly_path + filename3, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(hash_part_tree)

    with open(drectly_path + filenameB, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data_B)
    
    with open(drectly_path + filenameC, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data_C)
 
    return len(not_dup_partid), total_num
    

# 実行
if __name__ == "__main__":

    data = []
    
    for i in range(5):

        lengh = 0

        while lengh < NUM_part/2:
            part_tree, lengh = new_generate_part_tree(NUM_part, MIN_CHILDREN, MAX_CHILDREN)

        not_dup_num, total_num = wirte_csv(part_tree, 'relations.csv', 'info.csv', 'hash_part.csv', 'offB.csv', 'offC.csv', i)
        rate = 100 - not_dup_num/total_num * 100

        print(rate)
        data.append((not_dup_num, total_num, str(rate)))
    
    with open('./'+ str(NUM_part) + '/duplication_rate.txt', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)

# scp -i ~/.ssh/id_rsa_ogl -r ./un0724  haruka-h@oglsv.ogl.is.ocha.ac.jp:~
# ssh -i ~/.ssh/id_rsa_ogl haruka-h@oglsv.ogl.is.ocha.ac.jp
# scp -r ~/un0724 haruka-h@192.168.100.15:/home/haruka-h/results/
# ssh -v haruka-h@192.168.100.15
# docker cp ~/results/un0724 postgresA:/root/results/