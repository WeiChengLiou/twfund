#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import MongoClient
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
    client = MongoClient(uri)
    return eval("client.%s" % dic['db'])

cn = init(CONFIG['db'])
