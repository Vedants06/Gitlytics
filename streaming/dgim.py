#!/usr/bin/env python3
"""
DGIM Algorithm for counting 1s in a sliding stream window (Experiment 6 / Module 4.5).
"""


class DGIMBucket:
    def __init__(self, timestamp: int, size: int):
        self.timestamp = timestamp
        self.size = size


class DGIM:
    def __init__(self, N: int):
        self.N = N  # Window size
        self.timestamp = 0
        self.buckets = []  # List of DGIMBucket sorted by timestamp (newest first)

    def update(self, bit: int):
        self.timestamp = (self.timestamp + 1) % (2 * self.N)

        # 1. Drop buckets older than sliding window N
        cutoff = (self.timestamp - self.N) % (2 * self.N)
        self.buckets = [b for b in self.buckets if self._is_active(b.timestamp, cutoff)]

        if bit == 0:
            return

        # 2. Add new bucket of size 1
        new_bucket = DGIMBucket(self.timestamp, 1)
        self.buckets.insert(0, new_bucket)

        # 3. Merge cascading buckets if we exceed 2 buckets of any size
        self._cascade_merge()

    def _is_active(self, ts: int, cutoff: int) -> bool:
        # Handles circular buffer wrapping
        if self.timestamp >= cutoff:
            return ts > cutoff and ts <= self.timestamp
        return ts > cutoff or ts <= self.timestamp

    def _cascade_merge(self):
        idx = 0
        while idx < len(self.buckets):
            size = self.buckets[idx].size
            # Count buckets of the same size
            same_size_buckets = [i for i, b in enumerate(self.buckets) if b.size == size]

            if len(same_size_buckets) > 2:
                # Merge the oldest two of this size (last two indices in same_size_buckets)
                first_to_merge_idx = same_size_buckets[-2]
                second_to_merge_idx = same_size_buckets[-1]

                # Merge them: size doubles, keep timestamp of the newer one
                self.buckets[first_to_merge_idx].size *= 2
                self.buckets.pop(second_to_merge_idx)
                # Loop continues to check next size layer
            else:
                break
            idx += 1

    def count(self) -> int:
        """Estimate the sum of 1s in the window."""
        if not self.buckets:
            return 0

        total = 0
        for i, b in enumerate(self.buckets):
            if i == len(self.buckets) - 1:
                # Add half of the oldest bucket's size
                total += b.size // 2
            else:
                total += b.size
        return total