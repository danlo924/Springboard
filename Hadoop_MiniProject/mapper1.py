#!/usr/bin/env python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # [derive mapper output key values]
    line = line.strip()
    line = line.split(",")
    vin = line[2]
    vals = [line[1],line[3],line[5]]
    print (vin, vals)