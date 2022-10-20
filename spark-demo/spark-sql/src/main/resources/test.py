#!/usr/bin/python
import sys

for line in sys.stdin:
    d=line.strip().split(',')
    if len(d) !=2:
        continue
    label=d.pop(0)
    hit_id=d.pop(0)
    features=[]
    features.append(("Item.id,%s" % label, 1))
    features.append(("Item.id,%s" % hit_id,1))
    print(features)