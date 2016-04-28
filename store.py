#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import json

from bson.objectid import ObjectId

from utils.message import redis_conn
from utils.mongo_db import mongo_conn, mongo_writer
from StorageManager import StorageManager


def extract_field_content(db, field):
    return (field.get('value') if not isinstance(field.get('value'), ObjectId)
            else db[field['name']].find_one({'_id': field.get('value')}))


def store(msg, db, librarian):
    table = msg['collection']
    filtre = msg['filtre']
    obj_to_store = db[table].find_one(filtre)
    new_obj = {}

    if not obj_to_store:
        print "Object not found, check the condition or maybe it's already been proceeded"

    for field in obj_to_store:
        # get target content
        content = (field.get('value') if not isinstance(field.get('value'), ObjectId)
                   else db[field['name']].find_one({'_id': field.get('value')}))
        if not content:
            raise NameError("Object information not completed!")

        """
        look for {field: storage} mapping storage set by user.
        if {field: storage} mapping not found, try to find {type: storage} mapping set by default.
        if {type: storage} mapping not found, try default storage.
        """
        storage = ((db.field_storage_mapping.find_one({'field': field.get('name')}) or {}).get('storage') or
                   (db.type_storage_mapping.find_one({'type': field.get('type')}) or {}).get('storage') or
                   librarian.get_default_storage('db'))
        if not storage:
            raise ValueError("No storage available!")

        # store 'content' into 'storage', keep address of stored object
        address = librarian.write(storage, content, 'db')

        # generate a new object with all fields and the storage
        new_obj[field.get('name')] = {'storage': storage,
                                      'id': address}

    db[table].find_one_and_replace(filtre, new_obj)


def run(db, redis, storage_manager):
    subscription = redis.pubsub(ignore_subscribe_messages=True)
    subscription.subscribe('read', 'write')

    for msg in subscription.listen():
        if msg.channel == 'write':
            store(json.loads(msg['data'].decode()), db, storage_manager)

if __name__ == '__main__':
    db_ = mongo_conn()
    redis_ = redis_conn()
    storage_manager_ = StorageManager()
    storage_manager_.add_database(db_, 'mongo_db', db_type='noSQL/document', write_handler=mongo_writer)
    run(db_, redis_, storage_manager_)


