#!/usr/bin/env python

import fileinput
import json
import pprint

order = [
    "class",
    "stream",
    "expver",
    "date",
    "time",
    "domain",

    "type",
    "levtype",
    "origin",
    "product",
    "section",
    "method",
    "system",
    "grid", # Not sure about this one
    "refdate",
    "hdate",
    "anoffset",
    "reference",

    "step",
    "fcmonth",
    "fcperiod",
    "leadtime",
    "opttime",
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

S = {}


for line in fileinput.input():
    n = line.find("Could not find a rule to archive")
    line = line.rstrip()
    if n >= 0 and line[-1] == '}':
        r = {}
        for x in line[n + 34:-1].split(','):
            k, v = x.split('=')
            r[k] = v

        k = tuple(sorted(r.keys(), cmp=lambda a, b: O[a]-O[b]))

        S.setdefault(r['stream'], {})
        S[r['stream']].setdefault(k, set())
        S[r['stream']][k].add(r['type'])

# pprint.pprint(S)
# exit(0)

U = {}

TTT = {}
for k, v in sorted(S.items()):
    # print k
    z = []
    diff = set()
    TT = {}

    gone = set()

    p = sorted(v, cmp=lambda a,b: len(b)-len(a))

    for a in p:

        if a in gone:
            continue

        types = v[a]

        for b in p:
            if b in gone:
                continue
            if a is not b:
                if set(b).issubset(set(a)):
                    # print a
                    # print b
                    # print
                    diff.update(set(a).difference(set(b)))
                    types.update(v[a])
                    types.update(v[b])
                    gone.add(b)
                # if set(a).issubset(set(b)):
                #     diff.update(set(b).difference(set(a)))
                #     types.update(v[a])
                #     types.update(v[b])
                #     gone.add(b)

        z.append(a)
        TT[tuple(a)] = types

    b = []
    for r in z:
        t = []
        for s in r:
            if s in diff:
                t.append(s + '?')
            else:
                t.append(s)
        b.append(tuple(t))
        TTT[(k, tuple(t))] = TT[tuple(r)]

    U[k] = tuple(b)

# print json.dumps(U, indent=4)
# exit(0)

P = {}
for k1, v1 in sorted(U.items()):
    same = set([k1])
    for k2, v2 in sorted(U.items()):
        if v1 == v2:
            same.add(k2)

    P[tuple(same)] = v1

for k1, v1 in sorted(P.items()):

    X = {}
    if len(k1) == 1 and len(v1) != 1:
        a = set()
        for r in v1:
            a.update(TTT[(list(k1)[0], r)])
        if len(a) != 1:
            for r in v1:
                X[tuple(r)] = TTT[(list(k1)[0], r)]

    print "#", "/".join(sorted(k1))
    lev0 = O['domain']
    lev1 = O['reference']

    for r in v1:
        x = []
        level = 0

        for k in r:
            if k == 'stream':
                v = "/".join(sorted(k1))
                x.append("stream=%s" % (v, ))
            elif k == 'type' and X.get(tuple(r)):
                v = "/".join(sorted(X.get(tuple(r), set())))
                x.append("type=%s" % (v, ))
            else:
                x.append(k)

            y = k
            if y.endswith('?'):
                y = y[:-1]

            if O[y] > lev0 and level == 0:
                p = x.pop()
                print "[", ", ".join(x)
                x = [p]
                level += 1

            if O[y] > lev1 and level == 1:
                p = x.pop()
                print "       [", ", ".join(x)
                x = [p]
                level += 1
        print "               [", ", ".join(x), "]]]"
        print
    # print sorted(same)
    # for r in v1:
    #     print "    ", r, "[", "/".join(X.get(tuple(r), set())), "]"
    print

