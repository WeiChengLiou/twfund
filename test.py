#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twfund import makeindex
from twfund import boardnet
from twfund import bossnet1
from twfund import query

#makeindex.refresh()
#makeindex.defnew()
#bossnet1.appitems()
#bossnet1.update_boss()

name, target = u'王雪紅', '70394652'
name, target = u'連勝武', '54180900'
bosskey = u'\t'.join([name, target])
G = query.get_boss_network(bosskey)

print '---'
for k, v in G.node.iteritems():
    print k, v

for k1, k2 in G.edges_iter():
    w = G.get_edge_data(k1, k2)
    print k1, k2, w['weight']

print G.number_of_nodes(), G.number_of_edges()
