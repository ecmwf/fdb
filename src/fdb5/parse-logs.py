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

TYPES={}
STREAMS={}
S={}
T={}

ALLT = set()
ALLS = set()

for line in fileinput.input():
    n = line.find("Could not find a rule to archive")
    line = line.rstrip()
    if n >= 0 and line[-1] == '}':
        r = {}
        for x in line[n + 34:-1].split(','):
            k, v = x.split('=')
            r[k] = v
        
        k = tuple(sorted(r.keys(), cmp=lambda a,b: O[a]-O[b]))       
        v = (r['stream'], r['type'], r.get('levtype',''))
        E.setdefault(k, set())
        E[k].add(v)

        S.setdefault(r['stream'], {})
        S[r['stream']][k] = r

        T.setdefault(k, {})
        T[k][(r['stream'], r['type'])] = r


        ALLT.add(r['type'])
        ALLS.add(r['stream'])

        for n in r.keys():
            TYPES.setdefault(n, set())
            STREAMS.setdefault(n, set())
            TYPES[n].add(r['type'])
            STREAMS[n].add(r['stream'])

"""
print

for k, v in sorted(E.items()):
    print k 
    print "    ", sorted(v)


print

for k, v in sorted(TYPES.items()):
    if v != ALLT:
        print k 
        print "    ", sorted(v)

print

for k, v in sorted(STREAMS.items()):
    if v != ALLS:
        print k 
        print "    ", sorted(v)

print "-----------------------------------"
"""
for k, v in sorted(T.items()):
    print k 
    for r in  sorted(v):
        print "    ", r
        #print "    ", v[r]
    print

