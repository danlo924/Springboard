#!/usr/bin/env python
import sys
import ast

# [Define group level master information]
current_vin = None
vin = None
vin_info = None

def reset():
    # [Logic to reset master info for every new group]
    global vin_info
    vin_info = None

# Run for end of every group
def flush(current_vin, vin_info):
    print(current_vin, vin_info)

# input comes from STDIN
for line in sys.stdin:
    # [parse the input we got from mapper and update the master info]
    line = line.strip()
    vin, info = line.split(' ',1)
    info = ast.literal_eval(info)

    # [detect key changes]
    if current_vin != vin:
        if current_vin != None:
            # write result to STDOUT
            flush(current_vin, vin_info)
        reset()

    # [update more master info after the key change handling]
    current_vin = vin
    if info[0] == 'I':
        vin_info = [info[1],info[2]]

# do not forget to output the last group if needed!
flush(current_vin, vin_info)