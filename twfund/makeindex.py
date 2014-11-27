#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import itertools as it
from traceback import print_exc
from pdb import set_trace
import yaml
from work import *
from utils import *
from pymongo.errors import DuplicateKeyError

CONFIG = yaml.load(open('config.yaml'))
path = '{0}/details'.format(CONFIG['src'])
print path

errcol = {}
errid = {}


# 取得 json 欄位定義
filedic = yaml.load(open('filespec.yaml'))

# 設定想留在資料庫的欄位
tblcol = yaml.load(open('tblcol.yaml'))


def dprint(dic):
    for k, v in dic.iteritems():
        print k, v


def sprint(li):
    for r in li:
        print u'\t'.join(r)


class FundItem(object):
    funderrdic = {}

    def __init__(self):
        """"""

    def read(self, kv):
        coldic = filedic['fundinfo']
        for k, v in kv.iteritems():
            if k == u'董監事':
                self.boards = v
            elif k == u'分事務所':
                if v != []:
                    self.branch = [x.split(u'：') for x in v]
                    if len(self.branch[0]) != 2:
                        self.branch = [(u'', x[0]) for x in self.branch]
                else:
                    self.branch = []
            else:
                key = coldic.get(k)
                if not key:
                    # 遇到異常欄位就要先註記再即時 debug
                    if k not in errcol.keys():
                        # 列出異常欄位名稱與公司統編
                        print u'Unknown Column:', k, v
                    print '???'
                    set_trace()
                else:
                    self.process(key, v)
        return self

    def process(self, key, v):
        if key == 'name':
            #v = replaces(v, [u'\u3000', u'。']). \
            #    replace(u'︵', u'（').replace(u'︶', u'）'). \
            #    replace(u'台灣', u'臺灣').replace(u'証券', u'證券')

            li = [(u'︵', u'（'),
                  (u'︶', u'）'),
                  (u'台灣', u'臺灣'),
                  (u'証券', u'證券'),
                  (u'\u3000', u' '),
                  (u'。', u'')]
            v = reduce(lambda x, y: x.replace(*y), li, v).strip()

            if v in FundItem.funderrdic:
                v = FundItem.funderrdic[v]
        self.__dict__[key] = v

    def insinfo(self):
        # 依 fundinfo 內容輸入公司基本資料
        self.dealboards()
        self.dealbranch()

        items = {}
        for col in filedic['fundinfo'].values():
            if not hasattr(self, col):
                continue
            items[col] = self.__dict__[col]

        if hasattr(self, 'boards'):
            # 新增董監事人數
            items['boards'] = self.boards
            items['boardcnt'] = self.boardcnt
        if hasattr(self, 'branch'):
            items['branch'] = self.branch
            items['branchcnt'] = self.branchcnt

        try:
            cn.fundinfo.insert(items)
        except DuplicateKeyError, e:
            """"""
        except:
            print_exc()
            set_trace()

    def dealboards(self):
        # 輸入董監事資料
        if 'boards' not in self.__dict__:
            return

        # 各公司董監事名單使用 set 儲存，因為可能有重複
        sets = set()
        cnt, li = 0, []
        for boss in self.boards:
            if (boss[1] == u'') or (boss[0] == u''):
                continue

            vs = [x.strip() for x in boss]
            kvs = boss[1]
            if kvs in sets:
                # 另外紀錄董監事名單裡重複的名字
                print kvs
            else:
                sets.add(kvs)
                li.append(vs)
                cnt += 1

        self.boards = li
        self.boardcnt = cnt

    def dealbranch(self):
        # insert branch data
        if 'branch' not in self.__dict__:
            return

        sets = set()
        cnt, li = 0, []
        for br in self.branch:
            if (br[1] == u''):
                continue
            vs = [x.strip() for x in br]
            kvs = br[1]
            if kvs in sets:
                # 另外紀錄分公司名單裡重複的名字
                print kvs
            else:
                sets.add(kvs)
                li.append(vs)
                cnt += 1

        self.branch = li
        self.branchcnt = cnt


def getfis():
    def fun(r):
        for x in r[2]:
            yield os.path.join(r[0], x)

    iter1 = it.ifilter(lambda x: x[2] != [], os.walk(path))
    return it.chain.from_iterable(it.imap(fun, iter1))


def instbl(kv):
    # 處理每筆 json item 至資料庫
    FundItem().read(kv).insinfo()


def runjobs(*args):
    fun = lambda fi: json.load(open(fi))
    # 逐檔案、逐 json item 執行給定函數
    fis = getfis()
    kvs = it.imap(fun, fis)
    f1 = lambda kv: map(lambda fun: fun(kv), args)
    map(f1, kvs)


def refresh():
    # 新增、處理資料庫
    cn.fundinfo.drop()
    cn.fundinfo.ensure_index([('name', 1), ('logkey', 1), ('logno', 1)], unique=True)
    cn.boards.drop()
    #cn.boards.ensure_index(
    #    [('title', 1), ('name', 1)],
    #    unique=True)
    cn.branch.drop()

    loaderrdic()

    runjobs(instbl)

    #fixing()


def loaderrdic():
    f = open('fixfund.txt')
    dic = {}
    for li in f:
        rs = li[:-1].decode('utf8').split(u':')
        dic[rs[0]] = rs[1]
    FundItem.funderrdic = dic


def insdb(dic):
    # fundinfo
    fun = lambda x: x[0] not in ('_id', 'boards', 'branch')
    dic1 = {k: v for k, v in it.ifilter(fun, dic.iteritems())}
    try:
        cn.fundinfo1.insert(dic1)
    except:
        print_exc()
        set_trace()

    # boards
    cols = ['title', 'name']
    for r in dic['boards']:
        vs = {'fund': dic['name'],
              'logkey': dic['logkey'],
              'logno': dic['logno']}
        for c, v in zip(cols, r):
            vs.__setitem__(c, v)
        cn.boards1.insert(vs)

    # branch
    cols = ['branch', 'addr']
    for r in dic['branch']:
        vs = {'fund': dic['name'],
              'logkey': dic['logkey'],
              'logno': dic['logno']}
        for c, v in zip(cols, r):
            vs.__setitem__(c, v)
        cn.branch1.insert(vs)


def defnew():
    def update(tbl, cond):
        tb0 = eval('cn.%s' % tbl)
        tb1 = eval('cn.%s1' % tbl)
        res = tb0.find_one(cond)
        if res:
            res.pop('_id')
            tb1.insert(res)

    cn.fundinfo1.drop()
    cn.branch1.drop()
    cn.boards1.drop()

    df = getdf(cn.fundinfo.find(
        {'name': {'$ne': ''}, 'logdate': {'$ne': ''}},
        {'name': 1, 'logkey': 1, 'logdate': 1, '_id': 0}))
    #df.sort(['name', 'pubdate'], inplace=True)
    df['maxy'] = df.T.apply(lambda x: int(x['logkey'][:3]))
    df['maxy'] = df[['maxy', 'logdate']].T.apply(
        lambda x: x['maxy'] if x['maxy'] >= 37 else int(x['logdate'][:3]))
    df['lastdate'] = df.T.apply(
        lambda x: '{0:03}{1}'.format(x['maxy'], x['logdate'][3:]))
    df.sort('lastdate', inplace=True)

    li = []
    for name, r in df.groupby('name').last().iterrows():
        li.append((name, r['logkey']))
    for item in chunk(li, 500):
        funds, logkeys = zip(*item)
        ret = cn.fundinfo.find({
            'name': {'$in': funds},
            'logkey': {'$in': logkeys}
            })
        for k, r in getdf(ret).groupby(['name', 'logkey']):
            if k not in li:
                continue
            idx = r.index[-1]
            insdb(r.ix[idx, :].to_dict())

    print 'retain {0} new items'.format(len(li))

    return df


if __name__ == '__main__':
    """"""
    ids = [u'75370905', u'16095002', u'73251209', u'75370601']
    #refresh()

