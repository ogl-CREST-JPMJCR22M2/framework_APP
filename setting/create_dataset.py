import csv
import random
import hashlib
import os
from collections import deque

# 定数の定義
NUM_part = 3000  # 部品の総部品数
MIN_CHILDREN = 0  # 子供の最小数
MAX_CHILDREN = 10  # 子供の最大数


# 部品木を生成する
def generate_part_tree(num_part, min_children, max_children):
    from collections import defaultdict

    part = [('P0', 'null', 1)]  # 初期ルートノード
    queue = [('P0', set(['P0']))]  # (parent_id, ancestor_set)

    used_edges = set()
    parent_map = defaultdict(set) 
    next_part_id = 1

    qty_weights = [1, 2, 4] # その部品が何個使われるか
    qty_probs = [0.99, 0.005, 0.005]  # qty_weightsのそれぞれの発生確率: 9:0.5:0.5

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

            if random.random() < 0.001 and reuse_candidates and total_nodes > NUM_part/2: # 一度登場した部品を再度選ぶ
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



# 部品木内の正確な総部品数を調べる
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





# CSVファイルを出力する
def wirte_csv(part_tree, num):

    assemblers = ['A', 'B', 'C']

    for a in assemblers:
        exec('{} = {}'.format('assembler' + a, [['partid', 'assembler']]))
        exec('{} = {}'.format('parts_tree' + a, [['partid', 'parents_partid', 'qty']]))
        exec('{} = {}'.format('cfpval' + a, [['partid', 'cfp', 'co2']]))

    hash_parts_tree = [['partid', 'hash']]

    all_type_of_parts = set()

    for i in part_tree:
        all_type_of_parts.add(i[0])

    all_type_of_parts = list(all_type_of_parts)
    total_num = count_total_parts(part_tree)

    for p in all_type_of_parts:

        assembler = random.choice(assemblers)
        cfpval = round(random.uniform(0.1, 1.0), 4)

        if p == 'P0' : 
            assembler = 'A'
            
        else : 
            exec('assembler{}.append([p, assembler])'.format(assembler))
            exec('cfpval{}.append([p, cfpval, cfpval])'.format(assembler))
            exec('parts_tree{}.append([i[0], i[1], i[2]])'.format(assembler))

        hash_parts_tree.append([p, 'null'])

    # directly作成
    drectly_path = './'+ str(NUM_part) + '/' + str(num)  + '/'
    os.makedirs(drectly_path, exist_ok=True)

    for a in assemblers:

        with open(drectly_path + f'assembler{a}', 'w', newline='') as f:
            writer = csv.writer(f)
            exec('writer.writerows(assembler{})'.format(a))

        with open(drectly_path + f'cfpval{a}', 'w', newline='') as f:
            writer = csv.writer(f)
            exec('writer.writerows(cfpval{})'.format(a))

        with open(drectly_path + f'parts_tree{a}', 'w', newline='') as f:
            writer = csv.writer(f)
            exec('writer.writerows(parts_tree{})'.format(a))
    
    with open(drectly_path + f'hash_parts_tree', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(hash_parts_tree)
 
    return len(all_type_of_parts), total_num
    

# 実行
if __name__ == "__main__":

    data = []
    
    for i in range(5):

        lengh = 0

        while lengh < NUM_part/2: # ランダム生成により総部品数が極端に少ないことがあるため，その場合にやり直す
            part_tree, lengh = generate_part_tree(NUM_part, MIN_CHILDREN, MAX_CHILDREN)

        not_dup_num, total_num = wirte_csv(part_tree, i)

        # 重複率を出力
        rate = not_dup_num/total_num * 100
        print(rate)
        data.append((not_dup_num, total_num, str(rate)))
    
    # 重複率を記録したduplication_rateのtxtファイルを出力
    with open('./'+ str(NUM_part) + '/duplication_rate.txt', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)

# scp -i ~/.ssh/id_rsa_ogl -r ./un0724  haruka-h@oglsv.ogl.is.ocha.ac.jp:~
# ssh -i ~/.ssh/id_rsa_ogl haruka-h@oglsv.ogl.is.ocha.ac.jp
# scp -r ~/un0724 haruka-h@192.168.100.15:/home/haruka-h/results/
# ssh -v haruka-h@192.168.100.15
# docker cp ~/results/un0724 postgresA:/root/results/