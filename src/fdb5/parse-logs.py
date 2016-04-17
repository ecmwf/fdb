#!/usr/bin/env python

import fileinput

order = [
    "class",
    "type",
    "stream",
    "levtype",
    "origin",
    "product",
    "section",
    "method",
    "system",
    "grid", # Not sure about this one
    "date",
    "refdate",
    "hdate",
    "time",
    "anoffset",
    "reference",
    "step",
    "fcmonth",
    "fcperiod",
    "leadtime",
    "opttime",
    "expver",
    "domain",
    "diagnostic",
    "iteration",
    "quantile",
    "number",
    "levelist",
    "latitude",
    "longitude",
    "range",
    "param",
    "ident",
    "obstype",
    "instrument",
    "frequency",
    "direction",
    "channel",]

O = {}
n = 0
for o in order:
    O[o] = n
    n += 1

E = {}

for line in fileinput.input():
    n = line.find("Could not find a rule to archive")
    line = line.rstrip()
    if n >= 0 and line[-1] == '}':
        r = {}
        for x in line[n + 34:-1].split(','):
            k, v = x.split('=')
            r[k] = v
        
        k = tuple(sorted(r.keys(), cmp=lambda a,b: O[a]-O[b]))       
        v = (r['stream'], r['type'], r['levtype'])
        E.setdefault(k, set())
        E[k].add(v)

for k, v in sorted(E.items()):
    print k 
    print "    ", v
