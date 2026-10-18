#!/usr/bin/env python3
"""
Hadoop Streaming MAPPER - revenue and number of purchases per brand.

Input : raw CSV lines of 2019-Nov.csv (one event per line)
Output: brand <TAB> 1 <TAB> price      (one line per purchase)

Runs with the python3 of the nodemanager container (Python 3.5): no f-strings.
"""
import io
import sys

stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8", errors="replace")
stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

for line in stdin:
    fields = line.rstrip("\n").split(",")
    # columns: event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
    if len(fields) != 9 or fields[1] != "purchase":      # also drops the header line
        continue
    try:
        price = float(fields[6])
    except ValueError:
        continue
    if price <= 0:
        continue
    brand = fields[5].strip() or "unknown"
    stdout.write("{}\t1\t{:.2f}\n".format(brand, price))

stdout.flush()
