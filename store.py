#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import json

from bson.objectid import ObjectId

from utils.message import redis_conn
from utils.file_storage import file_storage
from utils.mongo_db import mongo_conn, mongo_writer
from StorageManager import StorageManager


def store(msg, db, librarian):
    table = msg['collection']
    filtre = msg['filtre']

    # generate bson format of ObjectId from str type
    _id = filtre.get('_id')
    if _id:
        filtre.update({'_id': ObjectId(_id)})

    print 'look for object to store in database'
    obj_to_store = db[table].find_one(filtre, {'_id': 0})
    new_obj = {}

    if not obj_to_store:
        raise ValueError("Object not found, check the condition or maybe it's already been proceeded")

    print 'store fields'
    for field in obj_to_store:
        # get target content
        content = obj_to_store.get(field)
        if not content:
            raise NameError("Object information not completed!")

        # look for {field: storage} mapping storage set by user.
        # if {field: storage} mapping not found, try to find {type: storage} mapping set by default.
        # if {type: storage} mapping not found, try default database.
        storage = ((db.field_storage_mapping.find_one({'field': field}) or {}).get('storage') or
                   (db.type_storage_mapping.find_one({'type': obj_to_store[field].get('type')}) or {}).get('storage') or
                   librarian.get_default_storage('db'))
        if not storage:
            raise ValueError("No storage available!")

        # store 'content' into 'storage', keep address of stored object
        address = librarian.write(storage.get('name'), content, storage.get('type'), placement=field)

        # generate a new object with all fields and the storage
        new_obj.update({field: {'storage': storage.get('name'),
                                'address': address}})

    print 'replace original object by scheme-address mapping form'
    db[table].find_one_and_replace(filtre, new_obj)


def run(db, redis, storage_manager):
    subscription = redis.pubsub(ignore_subscribe_messages=True)
    subscription.subscribe('read', 'write')
    print "listening on channels"

    for msg in subscription.listen():
        if msg['channel'] == 'write':
            print 'write channel: {}'.format(msg)
            store(json.loads(msg['data']), db, storage_manager)
        print 'message proceeded'


if __name__ == '__main__':

    db_ = mongo_conn()
    redis_ = redis_conn()
    fs_ = file_storage()
    storage_manager_ = StorageManager()
    storage_manager_.add_database(db_, 'mongo_db', db_type='noSQL/document', write_handler=mongo_writer)
    storage_manager_.add_filesystem(fs_, 'local_fs', write_handler=fs_.write)
    run(db_, redis_, storage_manager_)


