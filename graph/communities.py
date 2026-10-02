#!/usr/bin/env python3
"""
Repository Co-Contribution Graph & Community Detection (Experiment 8 / Module 5.3).
1. Builds repo-repo graph from shared human contributors (Push, PR, Issues).
2. Applies Louvain Community Detection on the full graph.
3. Applies Girvan-Newman (Edge Betweenness) on a top-k subgraph for syllabus walkthrough.
4. Exports network edges and community nodes for R igraph visualization (Exp 7).
"""

import sys
import time
from collections import defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import community.community_louvain as community_louvain
import networkx as nx
from pymongo import MongoClient

from ingestion.config import MONGO_DB, MONGO_URI

EXPORT_DIR = REPO_ROOT / "exports"


def build_co_contribution_graph(min_edge_weight=2, max_events=500_000):
    """
    Extract human contributor events from MongoDB and build repo co-contribution graph.
    Two repos are connected if they share human contributors.
    """
    print(f"[{time.strftime('%X')}] Extracting human contributor interactions from MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    events = db["events"]

    query = {
        "type": {"$in": ["PushEvent", "PullRequestEvent", "IssuesEvent"]},
        "actor.login": {"$not": {"$regex": r"\[bot\]$", "$options": "i"}},
    }
    cursor = events.find(query, {"_id": 0, "actor.login": 1, "repo.name": 1}).limit(max_events)

    contributor_repos = defaultdict(set)
    repo_contributors = defaultdict(set)

    count = 0
    for doc in cursor:
        actor = doc["actor"]["login"]
        repo = doc["repo"]["name"]
        contributor_repos[actor].add(repo)
        repo_contributors[repo].add(actor)
        count += 1

    print(f"[{time.strftime('%X')}] Processed {count:,} contributor events.")

    # Calculate shared contributors between repos
    co_contributors = defaultdict(lambda: defaultdict(int))
    for actor, repos in contributor_repos.items():
        if len(repos) > 1 and len(repos) <= 50:  # avoid hyper-active multi-pushers
            repo_list = list(repos)
            for i in range(len(repo_list)):
                for j in range(i + 1, len(repo_list)):
                    r1, r2 = repo_list[i], repo_list[j]
                    co_contributors[r1][r2] += 1
                    co_contributors[r2][r1] += 1

    # Construct NetworkX Graph
    G = nx.Graph()
    for r1, neighbors in co_contributors.items():
        for r2, weight in neighbors.items():
            if weight >= min_edge_weight and r1 < r2:
                G.add_edge(r1, r2, weight=weight)

    print(f"[{time.strftime('%X')}] Graph constructed with {G.number_of_nodes():,} nodes (repos) and {G.number_of_edges():,} edges.")
    return G


def run_louvain_communities(G):
    """Run Louvain modularity optimization community detection."""
    print(f"\n[{time.strftime('%X')}] Running Louvain Community Detection...")
    partition = community_louvain.best_partition(G, weight="weight", random_state=42)
    modularity = community_louvain.modularity(partition, G, weight="weight")

    # Group repos by community
    communities = defaultdict(list)
    for repo, comm_id in partition.items():
        communities[comm_id].append(repo)

    sorted_comms = sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)

    print(f"[{time.strftime('%X')}] Louvain detected {len(communities):,} communities.")
    print(f"[{time.strftime('%X')}] Overall Graph Modularity: {modularity:.4f} (High community structure!)")

    print("\n" + "=" * 70)
    print("TOP 5 LARGEST REPO COMMUNITIES (LOUVAIN)")
    print("=" * 70)
    for idx, (comm_id, members) in enumerate(sorted_comms[:5], 1):
        print(f"Community #{comm_id} (Size: {len(members)} repos):")
        sample_repos = members[:4]
        print(f"  Sample repos: {', '.join(sample_repos)}")
        print("-" * 70)

    # Export community mapping
    comm_tsv = EXPORT_DIR / "repo_communities.tsv"
    with open(comm_tsv, "w") as f:
        f.write("repo\tcommunity_id\tcommunity_size\tdegree\n")
        for repo, comm_id in partition.items():
            size = len(communities[comm_id])
            deg = G.degree[repo]
            f.write(f"{repo}\t{comm_id}\t{size}\t{deg}\n")
    print(f"Saved Louvain community partitions to: {comm_tsv}")

    return partition


def run_girvan_newman_demo(G, top_k=30):
    """
    Girvan-Newman Edge Betweenness Clustering demonstration (Module 5.3 syllabus).
    Runs on the largest connected component of the top-k highest degree subgraph.
    """
    print(f"\n[{time.strftime('%X')}] Running Girvan-Newman Algorithm walkthrough on top subgraph...")

    # Extract top-k nodes by degree
    top_nodes = [n for n, d in sorted(G.degree(), key=lambda x: x[1], reverse=True)[:top_k]]
    subgraph = G.subgraph(top_nodes).copy()

    # Get largest connected component
    if not nx.is_connected(subgraph):
        largest_cc = max(nx.connected_components(subgraph), key=len)
        subgraph = subgraph.subgraph(largest_cc).copy()

    print(f"  Subgraph for Girvan-Newman: {subgraph.number_of_nodes()} nodes, {subgraph.number_of_edges()} edges")

    # Girvan-Newman step by step edge removals
    comp_generator = nx.algorithms.community.girvan_newman(subgraph)

    # Get first division (2 communities)
    first_split = tuple(sorted(c) for c in next(comp_generator))
    print(f"  [Step 1] First division created {len(first_split)} clusters by cutting highest betweenness edges:")
    for i, cluster in enumerate(first_split, 1):
        print(f"    Cluster {i} ({len(cluster)} repos): {', '.join(list(cluster)[:3])}...")

    # Get second division (3 communities)
    second_split = tuple(sorted(c) for c in next(comp_generator))
    print(f"  [Step 2] Second division created {len(second_split)} clusters.")


def export_graph_edges(G):
    """Export edges for R igraph visualization."""
    edges_tsv = EXPORT_DIR / "repo_graph_edges.tsv"
    with open(edges_tsv, "w") as f:
        f.write("source\ttarget\tweight\n")
        for u, v, data in G.edges(data=True):
            f.write(f"{u}\t{v}\t{data.get('weight', 1)}\n")
    print(f"Saved graph edges to: {edges_tsv}")


def main():
    EXPORT_DIR.mkdir(exist_ok=True)
    G = build_co_contribution_graph(min_edge_weight=2, max_events=500_000)
    if G.number_of_nodes() > 0:
        run_louvain_communities(G)
        run_girvan_newman_demo(G, top_k=30)
        export_graph_edges(G)
    else:
        print("Graph has 0 nodes with min_edge_weight=2. Try with min_edge_weight=1.")


if __name__ == "__main__":
    main()