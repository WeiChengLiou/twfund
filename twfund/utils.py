#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from pdb import set_trace
from traceback import print_exc
import logging
import yaml
from os.path import exists
logger = logging.getLogger('twfund')

# create console handler and set level to info
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# create error file handler and set level to error
handler = logging.FileHandler("twfund.log", "w", encoding=None, delay="true")
handler.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


CONFIG = yaml.load(open('config.yaml'))


def init(db=None):
    "init mongodb db class"
    pwdfi = '../pwd1.yaml'
    if not exists(pwdfi):
        pwdfi = 'pwd.example.yaml'
    dic = yaml.load(open(pwdfi))
    uri = 'mongodb://{user}:{pwd}@{ip}:{port}/{db}'.format(**dic)
    if db:
        dic['db'] = db
    else:
        dic['db'] = 'twfund'
    try:
        return MongoClient(uri)[dic['db']]
    except:
        print 'LOGIN ERROR:', uri
        raise Exception


cn = init(CONFIG['db'])


def insitem(cn, coll, item):
    try:
        cn[coll].insert(item)
    except DuplicateKeyError:
        """"""
    except:
        print_exc()
        set_trace()


