#!/usr/bin/env python3
"""
Flajolet-Martin algorithm for unique element estimation (Experiment 6 / Module 4.4).
Uses logarithmic zero-count averaging (LogLog/FM) with correction factor alpha = 0.77351.
"""

import mmh3

ALPHA = 0.77351  # Flajolet-Martin correction factor for logarithmic space averaging


class FlajoletMartin:
    def __init__(self, num_hashes: int = 64):
        self.num_hashes = num_hashes
        self.max_tail_zeroes = [0] * num_hashes

    def _get_trailing_zeroes(self, val: int) -> int:
        val = val & 0xFFFFFFFF
        if val == 0:
            return 32
        count = 0
        while (val & 1) == 0:
            count += 1
            val >>= 1
        return count

    def add(self, item: str):
        for i in range(self.num_hashes):
            hashed = mmh3.hash(item, i)
            zeroes = self._get_trailing_zeroes(hashed)
            if zeroes > self.max_tail_zeroes[i]:
                self.max_tail_zeroes[i] = zeroes

    def estimate(self) -> float:
        """
        Computes distinct count:
        1. Groups hashes into buckets of 8
        2. Computes the average zero count R_avg per bucket
        3. Computes bucket estimate: ALPHA * 2^(R_avg)
        4. Returns the median across bucket estimates
        """
        bucket_size = 8
        bucket_estimates = []

        for i in range(0, self.num_hashes, bucket_size):
            bucket_r = self.max_tail_zeroes[i : i + bucket_size]
            r_avg = sum(bucket_r) / len(bucket_r)
            # Correct formula: multiply by ALPHA
            est = ALPHA * (2 ** r_avg)
            bucket_estimates.append(est)

        bucket_estimates.sort()
        mid = len(bucket_estimates) // 2
        if len(bucket_estimates) % 2 == 1:
            return bucket_estimates[mid]
        return (bucket_estimates[mid - 1] + bucket_estimates[mid]) / 2.0