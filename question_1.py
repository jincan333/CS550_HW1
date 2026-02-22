"""
Question 1: People You Might Know - MapReduce Friendship Recommendation

This program simulates a MapReduce job to recommend friends based on
the number of mutual friends two users share.

Usage: python question_1.py [input_file] [output_file]
"""

import sys
from collections import defaultdict


def mapper(user, friend_list):
    """
    Map phase: For each user U with friends F = [f1, f2, ..., fn]:
    - Emit (fi, (fj, 1)) and (fj, (fi, 1)) for each pair (fi, fj) in F,
      meaning fi and fj share U as a mutual friend.
    - Emit (U, (fi, -1)) for each fi in F to mark existing friendships.
    """
    emissions = []
    for fi in friend_list:
        emissions.append((user, (fi, -1)))

    for i in range(len(friend_list)):
        for j in range(i + 1, len(friend_list)):
            fi, fj = friend_list[i], friend_list[j]
            emissions.append((fi, (fj, 1)))
            emissions.append((fj, (fi, 1)))

    return emissions


def reducer(user, values):
    """
    Reduce phase: For each user, aggregate mutual friend counts and
    filter out existing friends. Return top 10 recommendations sorted
    by mutual friend count (desc), then user ID (asc) for ties.
    """
    existing_friends = set()
    mutual_counts = defaultdict(int)

    for candidate, count in values:
        if count == -1:
            existing_friends.add(candidate)
        else:
            mutual_counts[candidate] += count

    for friend in existing_friends:
        mutual_counts.pop(friend, None)

    sorted_recs = sorted(mutual_counts.items(), key=lambda x: (-x[1], x[0]))
    return [uid for uid, _ in sorted_recs[:10]]


def main():
    input_file = sys.argv[1] if len(sys.argv) > 1 else "data/soc-LiveJournal1Adj.txt"
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    adjacency = {}
    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split("\t")
            user = int(parts[0])
            if len(parts) > 1 and parts[1]:
                friend_list = list(map(int, parts[1].split(",")))
            else:
                friend_list = []
            adjacency[user] = friend_list

    # Map phase
    intermediate = defaultdict(list)
    for user, friend_list in adjacency.items():
        for key, value in mapper(user, friend_list):
            intermediate[key].append(value)

    # Reduce phase
    results = {}
    for user in sorted(intermediate.keys()):
        recs = reducer(user, intermediate[user])
        results[user] = recs

    # Also include users with no recommendations
    for user in adjacency:
        if user not in results:
            results[user] = []

    # Output
    target_users = [924, 8941, 8942, 9019, 9020, 9021, 9022, 9990, 9992, 9993]

    if output_file:
        with open(output_file, "w") as f:
            for user in sorted(results.keys()):
                recs_str = ",".join(map(str, results[user]))
                f.write(f"{user}\t{recs_str}\n")
        print(f"Full output written to {output_file}")

    print("\nRecommendations for target users:")
    print("-" * 60)
    for user in target_users:
        recs = results.get(user, [])
        recs_str = ",".join(map(str, recs))
        print(f"{user}\t{recs_str}")


if __name__ == "__main__":
    main()
