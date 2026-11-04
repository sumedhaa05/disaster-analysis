#!/usr/bin/env python3
"""Hadoop Streaming mapper. Input: curated CSV. Output: event_type \t 1 \t casualties \t damage"""
import sys

for line in sys.stdin:
    p = line.rstrip("\n").split(",")
    if len(p) < 11:
        continue
    try:
        event_type = p[4]
        casualties = int(p[5]) + int(p[6])
        damage = float(p[7])
    except ValueError:
        continue
    print(f"{event_type}\t1\t{casualties}\t{damage}")
