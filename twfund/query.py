#!/usr/bin/env python
# -*- coding: utf-8 -*-

from utils import *
from work import *
import numpy as np
import itertools as it
import networkx as nx
from traceback import print_exc
from pdb import set_trace
import re
from collections import defaultdict
from vis import output as opt


# Basic{{{

# 建立董監網絡圖方法
# 1. 給定 name 與 target，查詢相關公司
# 2. 給定相關公司後，找出同間公司的董監名單
# 3. 根據不同 graph 需求，畫出同公司董監事間的 boss edge
# 4. 根據新的董監事名單，回到第一步，直到階數滿足為止。

def get_boss_network(names, maxlvl=1, **kwargs):
    # 逐級建立董監事網絡圖

    if not hasattr(names, '__iter__'):
        names = [names]

    G = kwargs.get('G', nx.Graph())
    comdic = kwargs.get('comdic', {})
    namedic = kwargs.get('namedic', {k: 0 for k in names})
    level = kwargs.get('level', 0)
    cn = kwargs.get('cn', init('twcom'))

# 1. 給定 name 與 target，查詢相關公司
    namekey, targets = zip(*map(invbosskey, names))
    ret = cn.bossnode.find({
        'name': {'$in': namekey},
        'target': {'$in': targets}})
    coms = []
    for r in ret:
        key = bosskey(r['name'], r['target'])
        if key not in names:
            continue
        [coms.append(x) for x in r['coms'] if x not in comdic]
        G.add_node(key, {'size': len(r['coms'])})
    [comdic.__setitem__(com, level) for com in coms]

# 2. 給定相關公司後，找出同間公司的董監名單
    if level < maxlvl:
        ret = cn.boards.find({'id': {'$in': coms}}).sort('id')
        for k, rs in it.groupby(ret, lambda x: x['id']):
            rs = list(rs)
            print k, len(rs)
            addbossedge(G, rs)
            for r in rs:
                key = bosskey(r['name'], r['target'])
                if key not in namedic:
                    namedic[key] = level+1

    if level == 0:
        kwargs['G'] = G
        kwargs['comdic'] = comdic
        kwargs['namedic'] = namedic
        kwargs['cn'] = cn
        for lvl in xrange(1, maxlvl+1):
            kwargs['level'] = lvl
            ids1 = [key1 for key1, lvl1 in it.ifilter(
                    lambda x: lvl == x[1], namedic.iteritems())]
            get_boss_network(ids1, maxlvl=maxlvl, **kwargs)

    return G


def addbossedge(G, rs):
    # 3. 根據不同 graph 需求，畫出同公司董監事間的 boss edge
    for r1, r2 in it.combinations(rs, 2):
        key1 = bosskey(r1['name'], r1['target'])
        key2 = bosskey(r2['name'], r2['target'])
        if G.has_edge(key1, key2):
            dic = G.get_edge_data(key1, key2)
            dic['weight'] += 1
        else:
            G.add_edge(key1, key2, {'weight': 1})


def get_network(ids, maxlvl=1, level=0, items=None, G=None):
    # 逐級建立公司網絡圖

    if not hasattr(ids, '__iter__'):
        ids = [ids]

    if G is None:
        G = nx.DiGraph()
        for r in ids:
            G.add_node(r)

    if items is None:
        items = {id: level for id in ids}

    newlvl = level + 1
    ret = cn.comivst.find({
        '$or': [{'src': {'$in': ids}},
                {'dst': {'$in': ids}}]})

    for r in ret:
        if (newlvl > maxlvl) and \
                not all([x in items for x in (r['src'], r['dst'])]):
            continue
        G.add_edge(r['src'], r['dst'],
                   {'width': r['seat']})

        if r['src'] not in items:
            items[r['src']] = newlvl
        if r['dst'] not in items:
            items[r['dst']] = newlvl

    if level == 0:
        for lvl in xrange(1, maxlvl+1):
            ids1 = [key1 for key1, lvl1 in it.ifilter(
                    lambda x: lvl == x[1], items.iteritems())]
            get_network(ids1, maxlvl=maxlvl,
                        level=lvl, items=items, G=G)

    return G


# }}}
# Support {{{


def getbosslike(names):
    # look for match boss given boss name
    if not hasattr(names, '__iter__'):
        names = [names]
    for name in names:
        ret = cn.bossnode.find({'name': name})
        for r in ret:
            yield bosskey(r['name'], r['target'])


def getidlike(names):
    # look for match id given company name
    if not hasattr(names, '__iter__'):
        names = [names]
    for name in names:
        ret = cn.iddic.find({'name': {'$regex': name}})
        for r in ret:
            if len(r['id']) > 1:
                logger.warning(u'Duplicate id: %s', name)
            yield {'name': r['name'], 'id': r['id'][0]}


def bosskey(name=None, comid=None):
    # get boss key
    assert(not (id is None and comid is None))
    return u'\t'.join([name, comid])


def invbosskey(key):
    # inverse boss key
    if key:
        return key.split(u'\t')


def get_boss(id):
    # get boss list by company id
    if not hasattr(id, '__iter__'):
        id = [id]
    for r in cn.boards.find({'id': {'$in': id},
                             'title': {'$not': re.compile(u'.*獨立.*')},
                             'name': {'$ne': u'缺額'}}):
        yield r


def getBoardbyID(ids):
    # get board list by company ids
    if not hasattr(ids, '__iter__'):
        ids = [ids]
    boards = cn.boards.find({'id': {'$in': ids}})
    df = getdf(boards)
    dic = {k: v[
        ['no', 'title', 'name', 'equity', 'repr_instid', 'repr_inst']
        ].sort('no') for k, v in df.groupby('id')}
    return dic


def getbosskey(name, id):
    # get boss key
    ret = cn.bossnode.find_one(
        {'name': name,
         'coms': {'$in': [id]}})
    if ret:
        return u'\t'.join((ret['name'], ret['target']))
    else:
        return None


def getcomboss(ids):
    # get bossnode by company id
    if not hasattr(ids, '__iter__'):
        ids = [ids]

    ret = cn.boards.find({'id': {'$in': ids}})
    for r in ret:
        yield bosskey(r['name'], r['target'])


def fillgrp(g, grps):
    for k, v in g.node.iteritems():
        v['group'] = 0
        for i, grp in enumerate(grps):
            if k in grp:
                v['group'] = i + 1
                break

# }}}
# for Export {{{


def cluster(xdic, k_clut=10):
    # Clustering label in k_clut clusters
    xs = sorted(xdic.values())
    N = len(xdic)
    x_uniq = np.unique(xs)
    if len(x_uniq) <= k_clut:
        x_uniq.sort()
        output = {k: round(float(np.argwhere(x_uniq == y).ravel()[0] + 1) /
                  len(x_uniq)*k_clut) for k, y in xdic.iteritems()}
    else:
        li = np.ones(k_clut, dtype=np.float64) * min(xs)
        j, target = 0, 0
        for i, y in enumerate(xs):
            p = float(i) / N
            if p >= target:
                li[j] = y
                j += 1
                target = np.float64(j) / k_clut
        try:
            li = np.unique(li)
            output = {k: round(float(np.argwhere(y >= li)[-1, 0] + 1) /
                      len(li)*k_clut) for k, y in xdic.iteritems()}
        except:
            set_trace()
    return output


def setnode(G, col, dic):
    # set each col's value with dict
    for k, v in dic.iteritems():
        if k in G.node:
            G.node[k][col] = v


def translate(vdic, dstlim, vlim=None):
    # translate vdic's value in dstlim scale
    vs = vdic.values()
    if vlim:
        vmin, vmax = vlim
    else:
        vmin = min(vs)
        vmax = max(vs)

    rngd = float(dstlim[1] - dstlim[0])
    rngv = float(vmax - vmin)
    r = rngd / rngv
    return {k: (dstlim[0] + r * float(v - vmin)) for k, v in vdic.iteritems()}


def exp_boss(G, **kwargs):
    # Export boss network
    # Fill info, translate com count into size
    # Cluster betweenness centrality into group
    # Export graph
    if len(G.node) == 0:
        return
    G = G.to_undirected()
    fill_boss_info(G)

    sizedic = {}
    for name, dic in G.node.iteritems():
        sizedic[name] = len(dic['coms'])

    deg = translate(sizedic, [5, 50])
    setnode(G, 'size', deg)

    ngrp = sum([v.get('group', 0) for v in G.node.values()])
    if ngrp == 0:
        output = cluster(nx.betweenness_centrality(G))
        setnode(G, 'group', output)

    return opt.exp_graph(G, **kwargs)


def exp_company(G, **kwargs):
    # Export company network
    # Fill info, translate degree centrality into size
    # Cluster betweenness centrality into group
    # Export graph
    if len(G.node) == 0:
        return
    fill_company_info(G)
    G1 = G.to_undirected(G)

    if any([('size' not in dic) for dic in G.node.values()]):
        try:
            degs = translate(nx.degree_centrality(G1), [8, 60])
        except:
            degs = {k: 8 for k in G.node}
        setnode(G, 'size', degs)

    if any([('group' not in dic) for dic in G.node.values()]):
        output = cluster(nx.betweenness_centrality(G1))
        setnode(G, 'group', output)

    return opt.exp_graph(G, **kwargs)


def showkv(id, name, info=None, board=None):
    # Prepare cominfo for tooltip
    s1 = []

    if info is None:
        s1.append(u'統一編號: %s' % id)
        s1.append(u'組織名稱: %s' % name)
    else:
        coldic = [('id', u'統一編號'), ('name', u'公司名稱'),
                  ('status', u'公司狀況'), ('eqtstate', u'股權狀況')]
        fun = lambda col: col in info
        for col, colname in it.ifilter(fun, coldic):
            s1.append(u'%s: %s' % (colname, unicode(info[col])))

    # Because board maybe None, or DataFrame,
    # but can also be empty DataFrame or not.
    # So combined two conditions here
    if (board is not None) and (len(board) > 0):
        for k, q in board.iterrows():
            s1.append(u' '.join(map(unicode, q)))
    else:
        s1.append(u'無董監事資料')

    return u'\n'.join(s1)


def fill_company_info(G):
    # Fill company info,
    # Remove company whose status not like '核准'.

    assert(len(G.node) > 0)
    ids = G.node.keys()
    dic = {'id': {'$in': ids}, 'title': {'$ne': u'法人代表'}}
    infos = {r['id']: r for r in cn.cominfo.find(dic)}
    boards = {k: v for k, v in getdf(cn.boards.find(dic)).groupby('id')}
    noderm = []
    for id, dic in G.node.iteritems():
        info = infos[id] if id in infos else None
        board = boards[id][
            ['no', 'title', 'name', 'equity', 'repr_instid', 'repr_inst']
            ].sort('no') if id in boards else None
        if info:
            name = info['name']
        else:
            name = id
        dic['name'] = name
        dic['tooltip'] = showkv(id, name, info, board)
        dic['capital'] = info['capital'] if info and 'capital' in info else 0
        if info and 'status' in info:
            dic['status'] = info['status']
            if u'核准' not in info['status']:
                noderm.append(id)
        else:
            dic['status'] = u''
        dic['name'] = fixname(dic['name'])

    G.remove_nodes_from(noderm)
    return G


def fill_boss_node(G, names, coms):
    # Fill boss node
    ret = cn.bossnode.find({
        'name': {'$in': list(names)},
        'coms': {'$in': list(coms)}})
    for r in ret:
        key = bosskey(r['name'], r['target'])
        if key in G.node:
            node = G.node[key]
            node['coms'] = r['coms']
            node['name'] = r['name']


def fill_boss_info(G):
    # Fill boss info
    names, ids = map(set, zip(*[x.split(u'\t') for x in G.node.keys()]))

    fill_boss_node(G, names, ids)
    [ids.update(x['coms']) for x in G.node.values()]
    ids = tuple(ids)
    ret = cn.boards.find({
        'name': {'$in': tuple(names)},
        'id': {'$in': ids}})
    dic = defaultdict(list)
    for r in ret:
        key = bosskey(r['name'], r['target'])
        dic[key].append((r['id'], r['title']))

    namedic = getnamedic(ids)
    for k, v in dic.iteritems():
        if k not in G.node:
            continue
        node = G.node[k]
        v = sorted(v)

        grpli = [k.split(u'\t')[0]]
        for id, grp in it.groupby(v, lambda x: x[0]):
            com = namedic[id]
            grp = tuple(grp)
            if len(grp) > 1:
                grpli.append(u'\n'.join(
                    [u'\t'.join([com, x[1]]) for x in grp
                        if x[1] != u'法人代表']
                ))
            else:
                grpli.append(u'\t'.join([com, grp[0][1]]))
        node['titles'] = grpli
        node['tooltip'] = u'\n'.join(grpli)
        node['size'] = len(node['coms'])

    for k, v in G.node.iteritems():
        assert('titles' in v)
    return G


# }}}
# {{{Advance search
w2 = u'\u202c'


def queryboss(name):
    #print u'{0},{1}|'.format(name, name[-1])
    #print (name.encode('utf8'))
    name = name.replace(w2, u'')
    ret = list(cn.bossnode.find({'name': name}, {'_id': 0}))
    ids = list(flatten([r['coms'] for r in ret]))
    dic = getnamedic(ids)
    dic = {r['target']: [dic[k] for k in r['coms']] for r in ret}
    return dic


def get_bossnet_boss(names, target=None, maxlvl=1):
    # get boss network from boss name
    if not target:
        names = list(getbosslike(names))
    else:
        names = [getbosskey(names, target)]
    g = get_boss_network(names, maxlvl)
    return g


def get_bossesnet(ids, maxlvl):
    # get boss network from company ids
    # fill boss info for export
    names = list(getcomboss(ids))
    G = get_boss_network(names, maxlvl=1)
    fill_boss_info(G)
    return G


def get_network_names(names, maxlvl=None):
    # get network by company's name
    ids = [x['id'] for x in getidlike(names)]
    if not maxlvl:
        maxlvl = 1
    return get_network(ids, maxlvl=maxlvl)


def get_network_boss(name, target=None, maxlvl=None):
    # get network by boss name
    # input:
    #   name: unicode, boss name
    # output: DiGraph
    
    name = name.replace(w2, u'')
    if not target:
        bosses = list(map(invbosskey, getbosslike(name)))
        names, targets = zip(*bosses)
        cond = {'name': name, 'target': {'$in': targets}}
    else:
        cond = {'name': name, 'target': target}
    coms, ids = [], []
    for r in cn.bossnode.find(cond):
        ids.extend(r['coms'])
        coms.append(r['coms'])

    if not maxlvl:
        maxlvl = 1
    g = get_network(ids, maxlvl=maxlvl)
    fillgrp(g, coms)
    return g


def get_network_comboss(id, maxlvl=None):
    # get network by boss in the same company
    bosses = [bosskey(r['name'], r['target'])
              for r in cn.boards.find({'id': id})]
    names, targets = zip(*map(invbosskey, bosses))
    ids = []
    for r in cn.bossnode.find({'name': {'$in': names},
                               'target': {'$in': targets}}):
        key = bosskey(r['name'], r['target'])
        if key in bosses:
            ids.extend(r['coms'])

    if not maxlvl:
        maxlvl = 1
    g = get_network(ids, maxlvl=maxlvl)
    fillgrp(g, [ids])
    return g


def get_network_comaddr(id, maxlvl=None):
    # get network from the same addr
    addr = None
    for r in cn.cominfo.find({'id': id}):
        addr = r['addr']

    ids = [r['id'] for r in cn.cominfo.find({'addr': addr})]
    if not maxlvl:
        maxlvl = 1
    g = get_network(ids, maxlvl=maxlvl)
    fillgrp(g, [ids])
    return g


# }}}


if __name__ == '__main__':
    """"""
