#!/usr/bin/env python
import sys

# [Define group level master information]
curr_car = None
tot_count = 0

def reset():
    # [Logic to reset master info for every new group]
    global tot_count
    tot_count = 0

# Run for end of every group
def flush(curr_car, tot_count):
    print(curr_car, tot_count)

# input comes from STDIN
for line in sys.stdin:
    # [parse the input we got from mapper and update the master info]
    line = line.strip()
    make_year, count = line.split('\t',1)

    try:
        count = int(count)
    except ValueError:
        pass

    # [detect key changes]
    if make_year == curr_car:
        tot_count += count
    else:
        if curr_car:
            # write result to STDOUT
            flush(curr_car, tot_count)
            reset()

        # [update more master info after the key change handling]
        curr_car = make_year
        tot_count = count

# do not forget to output the last group if needed!
flush(curr_car, tot_count)