#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twfund import makeindex
from twfund import bossnet1

makeindex.refresh()
makeindex.defnew()
bossnet1.appitems()
bossnet1.update_boss()

