from itertools import combinations
from collections import defaultdict

MIN_SUPPORT = 100
TOP_K = 5
DATA_PATH = "browsing.txt"

def load_transactions(path: str):
    """Each line is a transaction (browsing session). Items are space-separated."""
    transactions = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            transactions.append(set(line.split()))
    return transactions

def apriori_L1(transactions):
    """Return item support dict and L1 as sorted list of frequent items."""
    item_sup = defaultdict(int)
    for t in transactions:
        for x in t:
            item_sup[x] += 1
    L1 = sorted([x for x, c in item_sup.items() if c >= MIN_SUPPORT])
    return item_sup, L1

def apriori_L2(transactions, L1):
    """
    Generate candidate pairs C2 from frequent items L1 and count supports by scanning.
    Return pair support dict (for ALL counted pairs in C2) and L2 set of frequent pairs.
    """
    # C2 candidates from L1
    L1_set = set(L1)
    C2 = set()
    for a, b in combinations(L1, 2):
        C2.add((a, b))

    pair_sup = defaultdict(int)

    # Scan transactions once, only count pairs whose both items in L1
    for t in transactions:
        ft = sorted([x for x in t if x in L1_set])
        for a, b in combinations(ft, 2):
            p = (a, b)
            if p in C2:
                pair_sup[p] += 1

    L2 = set([p for p, c in pair_sup.items() if c >= MIN_SUPPORT])
    return pair_sup, L2

def apriori_C3_from_L2(L2):
    """
    Join step: generate candidate triples C3 from frequent pairs L2.
    Classic join: (a,b) and (a,c) -> (a,b,c) with b<c
    Prune: all 2-subsets of triple must be in L2.
    """
    # index pairs by first item
    by_first = defaultdict(set)
    for a, b in L2:
        by_first[a].add(b)

    C3 = set()
    for a, bs in by_first.items():
        bs = sorted(bs)
        for b, c in combinations(bs, 2):
            triple = (a, b, c)  # already sorted because a < b < c in our representation
            # prune: all pairs must be frequent
            if (a, b) in L2 and (a, c) in L2 and (b, c) in L2:
                C3.add(triple)

    return C3

def apriori_L3(transactions, C3):
    """Count supports for candidate triples C3 by scanning transactions; return triple_sup and L3."""
    triple_sup = defaultdict(int)
    if not C3:
        return triple_sup, set()

    # For fast subset checks: group candidates by their first item
    # But simplest: scan transaction triples and check membership in C3 (works fine usually)
    C3_set = set(C3)
    for t in transactions:
        ft = sorted(t)
        for tri in combinations(ft, 3):
            if tri in C3_set:
                triple_sup[tri] += 1

    L3 = set([tri for tri, c in triple_sup.items() if c >= MIN_SUPPORT])
    return triple_sup, L3

def top5_part_d(item_sup, pair_sup, L2):
    """Part (d): rules X=>Y and Y=>X for frequent pairs. Sort by conf desc; tie by LHS lexicographically."""
    rules = []  # (conf, lhs, rhs, sup_xy)

    for (a, b) in L2:
        sup_ab = pair_sup[(a, b)]
        rules.append((sup_ab / item_sup[a], a, b, sup_ab))
        rules.append((sup_ab / item_sup[b], b, a, sup_ab))

    # confidence desc, lhs lexicographically increasing (extra rhs for deterministic)
    rules.sort(key=lambda r: (-r[0], r[1], r[2]))
    return rules[:TOP_K]

def top5_part_e(pair_sup, triple_sup, L3):
    """
    Part (e): for each frequent triple (x,y,z), output:
      (x,y)=>z, (x,z)=>y, (y,z)=>x
    confidence = support(x,y,z)/support(pair)
    Sort by confidence desc; then order LHS pair lexicographically; tie by first then second item in pair.
    """
    rules = []  # (conf, (lhs1,lhs2), rhs, sup_xyz)

    for (x, y, z) in L3:
        sup_xyz = triple_sup[(x, y, z)]
        rules.append((sup_xyz / pair_sup[(x, y)], (x, y), z, sup_xyz))
        rules.append((sup_xyz / pair_sup[(x, z)], (x, z), y, sup_xyz))
        rules.append((sup_xyz / pair_sup[(y, z)], (y, z), x, sup_xyz))

    rules.sort(key=lambda r: (-r[0], r[1][0], r[1][1], r[2]))
    return rules[:TOP_K]

def main():
    transactions = load_transactions(DATA_PATH)
    item_sup, L1 = apriori_L1(transactions)
    pair_sup, L2 = apriori_L2(transactions, L1)
    C3 = apriori_C3_from_L2(L2)
    triple_sup, L3 = apriori_L3(transactions, C3)

    print("===== (d) Top 5 rules from frequent pairs =====")
    d_rules = top5_part_d(item_sup, pair_sup, L2)
    for i, (conf, lhs, rhs, sup) in enumerate(d_rules, 1):
        print(f"{i}. {lhs} => {rhs} | support={sup} | confidence={conf:.6f}")

    print("\n===== (e) Top 5 rules from frequent triples =====")
    e_rules = top5_part_e(pair_sup, triple_sup, L3)
    for i, (conf, (a, b), c, sup) in enumerate(e_rules, 1):
        print(f"{i}. ({a}, {b}) => {c} | support={sup} | confidence={conf:.6f}")

if __name__ == "__main__":
    main()