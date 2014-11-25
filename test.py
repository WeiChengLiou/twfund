#!/usr/bin/env python
# -*- coding: utf-8 -*-

from twfund import makeindex
from twfund import boardnet

makeindex.refresh()
makeindex.defnew()
boardnet.update_boss()
