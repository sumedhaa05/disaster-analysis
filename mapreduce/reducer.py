#!/usr/bin/env python3
"""Hadoop Streaming reducer: totals events, casualties and damage per event type."""
import sys

current, events, cas, dmg = None, 0, 0, 0.0

def flush():
    if current is not None:
        print(f"{current}\t{events}\t{cas}\t{dmg:.0f}")

for line in sys.stdin:
    key, n, c, d = line.rstrip("\n").split("\t")
    if key != current:
        flush()
        current, events, cas, dmg = key, 0, 0, 0.0
    events += int(n); cas += int(c); dmg += float(d)
flush()
