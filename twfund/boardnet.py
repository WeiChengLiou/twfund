#!/usr/bin/env python
# -*- coding: utf-8 -*-

from groups import groups
from pdb import set_trace
from traceback import print_exc
from collections import defaultdict
import itertools as it
from work import *
from utils import *
#import multiprocessing as mp
#pool = mp.Pool(processes=2)


def update_boss():
    """新增董監事資料表"""
    idsall = getcoms2()
    logger.info('Total foundation count: {0}'.format(len(idsall)))
    dup_bossname(idsall)
    dup_boardlist()

    names = cn.boards1.distinct('name')

    #fix_bad_board()
    run_bossnodes(names=names, reset=True)
    run_upd_bossedges(idsall)


def run_upd_boards(names=None):
    """update all boards"""
    step = 1000
    [upd_boards(x) for x in chunk(names, step)]

    update_boss()


def upd_boards(names):
    """update boards: integrate id and repr_inst information"""
    repli = []

    """return grouped records by names"""
    condic = {'name': {'$in': names}}
    ret = cn.boards1.find(condic)
    dic = groupdic(ret, key=lambda r: r['name'])

    for name, items in dic.iteritems():
        if name == u'':
            continue
        if items is None:
            print 'Error: ', name
            repli.append(name)
            continue

        df = groupdic(items, key=lambda r: r['fund'])
        grps = grouping(items)
        upd_board_target(name, df, grps)
    return repli


def upd_board_target(name, df, grps):
    """update boards info"""
    for grp in grps:
        target = None
        for id in grp:
            if target is None:
                target = id

            for r in df[id]:
                # r = df[id]
                if r.get('target') == target:
                    continue
                r['target'] = target
                if '_id' in r:
                    cn.boards1.save(r)
                else:
                    cn.boards1.insert(r)


def dup_bossname(comids):
    """save any two fund's duplicate names and count by pair"""
    print get_funname()
    cn.dupboss.drop()
    cn.dupboss.ensure_index([
        ('name', 1), ('fund1', 1), ('fund2', 2)], unique=True)

    rets = cn.boards1.find(
        {'fund': {'$in': comids},
         },
        {'fund': 1, 'name': 1, '_id': 0})
    dic = defaultdict(set)
    [dic[r['fund']].add(r['name']) for r in rets]

    for (id1, df1), (id2, df2) in it.combinations(dic.iteritems(), 2):
        namedup = df1.intersection(df2)
        if len(namedup) <= 1:
            continue

        dic = {
            'fund1': id1,
            'fund2': id2,
            'names': list(namedup),
            'cnt': len(namedup)
            }
        cn.dupboss.save(dic)


def dup_boardlist():
    """save any two fund's duplicate names by name"""
    print get_funname()
    cn.grpconn.drop()
    cn.grpconn.ensure_index(
        [('name', 1), ('fund1', 1), ('fund2', 2)],
        unique=True)

    ret = cn.dupboss.find()
    for r in ret:
        for name in r['names']:
            dic = {
                'name': name,
                'fund1': r['fund1'],
                'fund2': r['fund2']}
            cn.grpconn.save(dic)


def grouping(items, grps=None):
    """grouping items with key"""
    if grps is None:
        grps = groups()
    for r in items:
        grps.add(r['fund'])
    return grps


def fix_bad_board():
    """reset every bad names'target as id"""
    print get_funname()
    badnames = bad_board(cn)
    for r in cn.boards1.find({'name': {'$in': badnames}}):
        r['target'] = r['fund']
        cn.boards1.save(r)


def run_bossnodes(names=None, reset=False):
    """refresh all bossnodes"""
    print get_funname()
    if reset:
        cn.bossnode.drop()
        cn.bossnode.create_index([('name', 1), ('target', 1)], unique=True)

    step = 500
    if names is None:
        names = cn.boards1.distinct('name')
        logger.info('Total: {0}'.format(len(names)))

    # map(reset_bossnode, chunk(names, step))
    map(reset_bossnode, list(chunk(names, step)))
    print 'Final'


def reset_bossnode(names):
    """reset boss node by names"""
    repli = []
    ret = cn.boards1.find({'name': {'$in': names}})
    namedic = groupdic(ret, lambda r: r['name'])
    grpdic = defaultdict(groups)
    [grpdic[r['name']].add(r['fund1'], r['fund2'])
        for r in cn.grpconn.find({'name': {'$in': names}})]

    for name, items in namedic.iteritems():
        if items is None:
            repli.append(name)
            continue

        grps = grpdic[name]
        grouping(items, grps)
        df = groupdic(items, lambda r: r['fund'])
        upd_board_target(name, df, grps)
        # print grps

        for grp in grps:
            dic = {'name': name,
                   'funds': list(grp)}
            dic['target'] = dic['funds'][0]
            cn.bossnode.insert(dic)

    if repli != []:
        reset_bossnodes(repli)


def getcoms2():
    """get funds id"""
    condic = {'boardcnt': {'$gt': 1}, 'log_why': {'$in': [u'變更登記', u'設立登記']}}
    return cn.fundinfo.find(condic, ['name']).distinct('name')


def run_upd_bossedges(ids=None):
    """update all boss edges"""
    print get_funname()
    cn.bossedge.drop()
    cn.bossedge.ensure_index([('src', 1), ('dst', 1)])
    map(upd_bossedge, chunk(ids, 500))


def upd_bossedge(ids):
    """update boss edge by ids"""
    def getkey(r):
        return (r['name'], r['target'] if 'target' in r else r['fund'])

    def inskey(key0, key1):
        try:
            dic = {'src': u'\t'.join(key0),
                   'dst': u'\t'.join(key1),
                   'cnt': len(mandic[key0].intersection(mandic[key1]))}
            cn.bossedge.insert(dic)
        except:
            print_exc()
            set_trace()

    bossdic = {}
    ret = cn.boards1.find({
        'fund': {'$in': ids},
        '$or': [{'no': '0001'}, {'title': u'董事長'}]
        })
    [bossdic.__setitem__(r['fund'], getkey(r)) for r in ret]

    #mandic = {(r['name'], r['target']): set(r['coms']) for r in
    #          cn.bossnode.find({'coms': {'$in': ids}})}
    mandic = {}
    for r in cn.bossnode.find({'funds': {'$in': ids}}):
        mandic[(r['name'], r['target'])] = set(r['funds'])

    ret = cn.boards1.find({
        'fund': {'$in': ids}})
    for r in ret:
        key0 = getkey(r)
        if key0 not in mandic:
            continue
        if r['fund'] not in bossdic:
            ret = cn.boards1.find({
                'fund': r['fund']}).sort('no')
            for r in ret:
                bossdic[r['fund']] = getkey(r)
                break
            if r['fund'] not in bossdic:
                bossdic[r['fund']] = [getkey(r)]

        if key0 == bossdic[r['fund']]:
            continue
        key1 = bossdic[r['fund']]
        if key1 not in mandic:
            continue
        if isinstance(key1, list):
            for key in key1:
                inskey(key0, key)
                inskey(key, key0)
            bossdic[r['fund']].append(key0)
        else:
            inskey(key0, key1)


if __name__ == '__main__':
    names = [u'王文洋', u'余建新', u'羅智先', u'謝國樑', u'王貴雲', u'王雪紅']
    ids = [u'75370905', u'16095002', u'73251209', u'75370601']

    #run_upd_boards()
    #update_boss()

    #idsall = getcoms2()
    #dup_bossname(idsall)
    #dup_boardlist()
    #comids = ['89399262', '79837539']
    #dup_bossname(comids)
    #reset_bossnode()

