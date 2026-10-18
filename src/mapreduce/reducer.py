#!/usr/bin/env python3
"""
Hadoop Streaming REDUCER (also used as COMBINER) - sums purchases and revenue per brand.

Input : brand <TAB> n_purchases <TAB> revenue   (sorted by brand by the shuffle)
Output: brand <TAB> n_purchases <TAB> revenue   (same format, so it can be a combiner)

Runs with the python3 of the nodemanager container (Python 3.5): no f-strings.
"""
import io
import sys

stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

current_brand = None
count = 0
revenue = 0.0


def emit(brand, n, total):
    stdout.write("{}\t{}\t{:.2f}\n".format(brand, n, total))


for line in stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 3:
        continue
    brand, n, value = parts[0], int(parts[1]), float(parts[2])

    if brand != current_brand:          # a new key starts: the previous one is complete
        if current_brand is not None:
            emit(current_brand, count, revenue)
        current_brand, count, revenue = brand, 0, 0.0

    count += n
    revenue += value

if current_brand is not None:
    emit(current_brand, count, revenue)

stdout.flush()
