#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import json

from bson.objectid import ObjectId

from utils.message import redis_conn
from utils.file_storage import file_storage
from utils.mongo_db import mongo_conn, mongo_writer
from StorageManager import StorageManager


def store_field(db, field_name, field_content, storage_manager):
    if isinstance(field_content, dict) and isinstance(field_content.get('value', ''), list):
        # deal with multiple part by storing them and keeping only their address
        field_content_value = [store_field(db, 'part', part, storage_manager) for part in field_content['value']]

        # update 'value' as collection of stored parts' address; 'type' as dictionary type
        field_content.update({'value': field_content_value,
                              'type': list.__name__})

    """
    first look for {field: storage} mapping storage set by user.
    if {field: storage} mapping not found, try to find {type: storage} mapping set by default.
    if {type: storage} mapping not found, use default database.
    """
    storage = ((db.field_to_storage.find_one({'field': field_name}) or {}).get('storage') or
               (db.type_to_storage.find_one({'type': type(field_content.get('value')).__name__}) or {}).get('storage') or
               storage_manager.get_default_storage('db'))
    if not storage:
        raise ValueError("No storage available!")

    # store 'content' into 'storage', keep address of stored object
    address = storage_manager.write(storage['name'], field_content, storage['type'], placement=field_name)
    return {'storage': storage['name'],
            'address': address}


def store_object(db, original_object, target_table, storage_manager):
    """
    storage of object in db
    """
    target_object = {}
    for field in original_object:
        # get target content
        field_content = original_object.get(field)
        if not field_content:
            raise NameError("Object information not completed!")

        location = store_field(db, field, field_content, storage_manager)
        target_object.update({field: location})

    return db[target_table].insert_one(target_object).inserted_id


def write_handler(msg, db, storage_manager):
    table = msg.get('collection')
    filtre = msg.get('filtre', {})

    if not table:
        raise ValueError('Table to request not indicated!')

    target_table = msg.get('target_collection', 'new_' + table)

    # generate bson format of ObjectId from str type
    _id = filtre.get('_id')
    if _id:
        filtre.update({'_id': ObjectId(_id)})

    print 'looking for object to store in database'
    original = db[table].find_one(filtre, {'_id': 0})
    if not original:
        raise ValueError("Object not found, check the condition or maybe it's already been proceeded")

    return store_object(db, original, target_table, storage_manager)


def run(db, redis, storage_manager):
    subscription = redis.pubsub(ignore_subscribe_messages=True)
    subscription.subscribe('read', 'write')
    print "listening on channels"

    for msg in subscription.listen():
        if msg['channel'] == 'write':
            print "receive from 'write' channel: {}".format(msg)
            write_handler(json.loads(msg['data']), db, storage_manager)
        print 'message proceeded'


if __name__ == '__main__':

    db_ = mongo_conn()
    redis_ = redis_conn()
    fs_ = file_storage()
    storage_manager_ = StorageManager()
    storage_manager_.add_database(db_, 'mongo_db', db_type='noSQL/document', write_handler=mongo_writer)
    storage_manager_.add_filesystem(fs_, 'local_fs', write_handler=fs_.write)
    run(db_, redis_, storage_manager_)


