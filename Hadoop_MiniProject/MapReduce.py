#!/usr/bin/env python
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # [derive mapper output key values]
    print (line.rstrip())
    # print '%s\t%s' % (key, value)