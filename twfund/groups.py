#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pdb import set_trace


class groups(object):
    """Clustering different coms with com connections"""
    def __init__(self):
        self.groups = []

    def find_group(self, id):
        for grp in (self.groups):
            if id in grp:
                return grp
        return None

    def add(self, id, *args):
        id = unicode(id)
        grp = self.find_group(id)
        if grp is None:
            self.groups.append(set([id]))
            grp = self.find_group(id)

        for id1 in args:
            id1 = unicode(id1)
            grp1 = self.find_group(id1)
            if grp1 and (grp != grp1):
                grp.update(grp1)
                self.groups.remove(grp1)
            else:
                grp.add(id1)

    def __iter__(self):
        for r in self.groups:
            yield r

    def __str__(self):
        try:
            li = [u'Group count: {0}'.format(len(self.groups))]
            for grp in self.groups:
                li.append(u'[{0}]'.format(u'\t'.join(grp)))
            return u'\n'.join(li).encode('utf8')
        except:
            set_trace()


if __name__ == '__main__':
    """"""

