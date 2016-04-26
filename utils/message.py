#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import os

import redis


def redis_conn(conf=None):
    """Connect to redis.
    """
    conf = conf or os.environ

    opts = {
        'host': conf.get('__BROKER_HOST__', 'localhost'),
        'port': conf.get('__BROKER_PORT__', 6379)
    }

    print 'connecting to redis: {}'.format(opts)
    return redis.StrictRedis(**opts)


def redis_pubsub():
    """ Redis pubsub connection getter. """
    _redis_ = redis_conn()
    assert _redis_.ping()
    return _redis_.pubsub(ignore_subscribe_messages=True)
