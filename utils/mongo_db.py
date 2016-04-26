#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import os

from pymongo import MongoClient


def mongo_db_name(conf):
    # get mongo database name
    return conf.get('__DB_NAME__') or 'analytics'


def mongo_conn(conf=None):
    """ return a mongo connection
    """
    conf = conf or os.environ

    user = conf.get('__DB_USER__')
    pwd = conf.get('__DB_PASS__')
    addr = conf.get('__DB_ADDR__') or 'localhost:27017'

    mongo = MongoClient('mongodb://{}'.format(addr))

    if user and pwd:
        mongo[mongo_db_name(conf)].authenticate(user, password=pwd)

    return mongo[mongo_db_name(conf)]
