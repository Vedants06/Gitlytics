#!/usr/bin/env python3
"""
Bloom Filter for stream membership tests (Experiment 6).
Uses MurmurHash3 to generate multiple independent hash values.
"""

import math
from pathlib import Path
import mmh3


class BloomFilter:
    def __init__(self, expected_elements: int, false_positive_rate: float):
        self.n = expected_elements
        self.p = false_positive_rate

        # Optimal size m = - (n * ln(p)) / (ln(2)^2)
        self.m = int(- (self.n * math.log(self.p)) / (math.log(2) ** 2))
        self.m = max(self.m, 1)

        # Optimal number of hash functions k = (m/n) * ln(2)
        self.k = int((self.m / self.n) * math.log(2))
        self.k = max(self.k, 1)

        self.bit_array = [0] * self.m

    def _hashes(self, item: str):
        """Generate k hash indices for a given item using double hashing technique."""
        # Double hashing scheme: hash(i) = (hash1 + i * hash2) % m
        hash1 = mmh3.hash(item, 0)
        hash2 = mmh3.hash(item, 1)
        for i in range(self.k):
            yield abs(hash1 + i * hash2) % self.m

    def add(self, item: str):
        for index in self._hashes(item):
            self.bit_array[index] = 1

    def contains(self, item: str) -> bool:
        for index in self._hashes(item):
            if self.bit_array[index] == 0:
                return False
        return True


def run_bloom_experiment(known_bots_path: Path):
    """Run an experiment with different sizes to prove the false positive rate."""
    with open(known_bots_path, "r", encoding="utf-8") as f:
        bots = [line.strip() for line in f if line.strip()]

    # We split bots 50/50: half to train Bloom, half to test for false positives
    half = len(bots) // 2
    train_bots = set(bots[:half])
    test_bots_out = bots[half:]  # Elements NOT in the filter

    print(f"Bloom Experiment: Training on {len(train_bots)} bots. Testing on {len(test_bots_out)} external elements.")
    print(f"{'Target FPR':<12} {'Bit Array Size (m)':<20} {'Hashes (k)':<12} {'Actual FPR':<12} {'Theoretical FPR':<16}")
    print("-" * 75)

    results = []
    for target_fpr in [0.10, 0.05, 0.01, 0.001]:
        bf = BloomFilter(len(train_bots), target_fpr)
        for b in train_bots:
            bf.add(b)

        # Count false positives on elements NOT inserted
        false_positives = sum(1 for b in test_bots_out if bf.contains(b))
        actual_fpr = false_positives / len(test_bots_out) if test_bots_out else 0

        # Theoretical formula: (1 - e^(-kn/m))^k
        k, m, n = bf.k, bf.m, len(train_bots)
        theoretical_fpr = (1 - math.exp(-k * n / m)) ** k

        print(f"{target_fpr:<12.3f} {bf.m:<20,d} {bf.k:<12d} {actual_fpr:<12.5f} {theoretical_fpr:<16.5f}")
        results.append({
            "target_fpr": target_fpr,
            "m": bf.m,
            "k": bf.k,
            "actual_fpr": actual_fpr,
            "theoretical_fpr": theoretical_fpr
        })

    return results