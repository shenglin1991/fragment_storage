#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

from bson.objectid import ObjectId

from utils.message import redis_conn
from utils.mongo_db import mongo_conn
from StorageManager import StorageManager


def store(msg, db, storage_manager):
    table = msg.collection
    obj_to_store = db[table].find_one(msg.condition)

    if not obj_to_store:
        print 'Object not found, check the condition or maybe it\'s already been proceeded'

    for field in obj_to_store:
        # get target content
        if isinstance(field.get('value'), ObjectId):
            db[field.get('name')].find_one({'_id': field.get('value')})

        # look for mapping storage set by user
        storage_defined_by_user = db.field_storage_mapping.find_one({'field': field.get('name')})

        if not storage_defined_by_user:

            pass

        storage_manager.write(field.get('name'), content, stype='db')
    return


def run(db, redis, storage):
    subscription = redis.pubsub(ignore_subscribe_messages=True)
    subscription.subscribe('read', 'write')

    for msg in subscription.listen():
        store(msg, db, storage)

if __name__ == '__main__':
    _db = mongo_conn()
    _redis = redis_conn()
    _storage_manager = StorageManager()
    run(_db, _redis, _storage_manager)


