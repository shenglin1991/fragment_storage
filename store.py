#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import json

from bson.objectid import ObjectId

from mongo_db import mongo_conn
from message import redis_conn
from StorageManager import StorageManager


def get_test_storage_manager(root_db):
    """
    Generate testing storage manager instance
    :param root_db: indicate root database
    :return: the testing storage manager with local filesystem and a mongoDB
    """
    from file_storage import file_storage
    from mongo_db import mongo_conn, mongo_writer

    fs_ = file_storage()
    db2 = mongo_conn({
        '__DB_ADDR__': 'localhost:27027',
    })
    storage_manager = StorageManager()
    storage_manager.add_database(root_db, 'mongo_db', db_type='noSQL/document', write_handler=mongo_writer)
    storage_manager.add_database(db2, 'mongo_db2', db_type='noSQL/document', write_handler=mongo_writer)
    storage_manager.set_default_storage('mongo_db2')
    storage_manager.add_filesystem(fs_, 'local_fs', write_handler=fs_.write)

    return storage_manager


class Store(object):
    def __init__(self, storage_manager=None, root_db=None):
        self.storage_manager = storage_manager
        self.root_db = root_db

    def store_field(self, db, field, content):
        if isinstance(content, dict) and isinstance(content.get('value', ''), list):
            # deal with multiple part by storing them and keeping only their address
            value = [self.store_field(db, 'multipart', part) for part in content['value']]

            # update 'value' as collection of stored parts' address; 'type' as dictionary type
            content.update({'value': value,
                            'type': list.__name__})

        """
        first look for {field: storage} mapping storage set by user.
        if {field: storage} mapping not found, try to find {type: storage} mapping set by default.
        if {type: storage} mapping not found, use default database.
        """
        storage = ((db.field_to_storage.find_one({'field': field}) or {}).get('storage') or
                   (db.type_to_storage.find_one({'type': type(content.get('value')).__name__}) or {})
                   .get('storage') or self.storage_manager.get_default_storage('db'))
        if not storage:
            raise ValueError("No storage available!")

        # store 'content' into 'storage', keep address of stored object
        address = self.storage_manager.write(storage['name'], content, storage['type'], placement=field)
        return {'storage': storage['name'],
                'collection': field,
                'address': address}

    def store_object(self, original_object, target_storage, target_table):
        """
        storage of object in db
        """
        target_object = {}
        for key, value in original_object.iteritems():
            location = self.store_field(self.root_db, key, value)
            target_object.update({key: location})

        return (self.storage_manager.write(target_storage, target_object, placement=target_table)
                if target_storage != 'n/a'
                else self.root_db[target_table].insert_one(target_object).inserted_id)

    def write_handler(self, msg):
        """
        Handle write operations
        :param msg: data part of message
        :return: write result
        """
        # get information from message
        table = msg.get('collection')
        filtre = msg.get('filtre', {})

        if not table:
            raise ValueError('Table to request not indicated!')

        target_table = msg.get('target_collection', 'new_' + table)
        target_storage = msg.get('target_storage', 'n/a')

        # generate bson format of ObjectId from str type
        _id = filtre.get('_id')
        if _id:
            filtre.update({'_id': ObjectId(_id)})

        print 'looking for object to store in database'
        original = self.root_db[table].find_one(filtre, {'_id': 0})
        if not original:
            raise ValueError("Object not found, check the condition or maybe it's already been proceeded")

        # store object found
        return self.store_object(original, target_storage, target_table)

    def run(self, redis):
        """
        Run redis message handler
        :param redis:   message bus for all operations
        """
        subscription = redis.pubsub(ignore_subscribe_messages=True)
        subscription.subscribe('read', 'write')
        print "listening on channels"

        for msg in subscription.listen():
            if msg['channel'] == 'write':
                print "receive from 'write' channel: {}".format(msg)
                self.write_handler(json.loads(msg['data']))
            print 'message proceeded'


def main(storage_manager=None):
    db_ = mongo_conn()
    redis_ = redis_conn()
    if storage_manager is None:
        storage_manager = get_test_storage_manager(root_db=db_)

    store = Store(storage_manager, root_db=db_)
    store.run(redis_)


if __name__ == '__main__':
    main()


