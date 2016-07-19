#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import json

from bson.objectid import ObjectId

from storages.mongo_db import mongo_conn
from storages.message import redis_conn
from StorageManager import StorageManager


def get_test_storage_manager(root_db):
    """
    Generate testing storage manager instance
    :param root_db: indicate root database
    :return: the testing storage manager with local filesystem and a mongoDB
    """
    from storages.file_storage import file_storage
    from storages.mongo_db import mongo_conn, mongo_writer, mongo_reader

    fs_hot = file_storage({'name': '/mnt/hotsemantic/'})
    fs_warm = file_storage({'name': '/mnt/warmsemantic/'})
    fs_cold = file_storage({'name': '/mnt/coldsemantic/'})
    db2 = mongo_conn({
        '__DB_ADDR__': 'localhost:27027',
    })
    storage_manager = StorageManager()

    storage_manager.add_database(root_db, 'mongo_db', db_type='noSQL/document',
                                 write_handler=mongo_writer,
                                 read_handler=mongo_reader)
    storage_manager.add_database(db2, 'mongo_db2', db_type='noSQL/document',
                                 write_handler=mongo_writer,
                                 read_handler=mongo_reader)
    storage_manager.set_default_storage('mongo_db2')
    storage_manager.add_filesystem(fs_hot, 'ceph_hot',
                                   write_handler=fs_hot.write,
                                   read_handler=fs_hot.read)
    storage_manager.add_filesystem(fs_warm, 'ceph_warm',
                                   write_handler=fs_warm.write,
                                   read_handler=fs_warm.read)
    storage_manager.add_filesystem(fs_cold, 'ceph_cold',
                                   write_handler=fs_cold.write,
                                   read_handler=fs_cold.read)

    return storage_manager


def is_multipart(content):
    return isinstance(content, dict) and isinstance(content.get('value', ''), list)


class Store(object):
    def __init__(self, storage_manager=None, root_db=None):
        self.storage_manager = storage_manager
        self.root_db = root_db

    def store_field(self, root_db, field, content):
        """
        first look for {field: storage} mapping storage set by user.
        if {field: storage} mapping not found, try to find {type: storage} mapping set by default.
        if {type: storage} mapping not found, use default database.
        :param root_db:
        :param field:
        :param content:
        :return:
        """
        if is_multipart(content):
            # deal with multiple part by storing them and keeping only their address
            value = [self.store_field(root_db, content.get('name', 'multipart'), part) for part in content['value']]

            # update 'value' as collection of stored parts' address; 'type' as list
            content.update({'value': value,
                            'type': list.__name__})

        # look for place to store the field
        storage = ((root_db.field_to_storage.find_one({'field': field}) or {}).get('storage') or
                   (root_db.type_to_storage.find_one({'type': content.get('type')}) or {})
                   .get('storage') or self.storage_manager.get_default_storage('db'))
        if not storage:
            raise ValueError("No storage available!")

        # store 'content' into 'storage', keep address of stored object
        # TODO : make placement more reasonable
        placement = content.get('name', field)
        address = self.storage_manager.write(storage['name'],
                                             content,
                                             storage['type'],
                                             placement=placement)
        return {'storage': storage['name'],
                'collection': placement,
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
        collection = msg.get('collection')
        filtre = msg.get('filtre', {})

        if not collection:
            raise ValueError('Table to request not indicated!')

        target_table = msg.get('target_collection', 'new_' + collection)
        target_storage = msg.get('target_storage', 'n/a')

        # generate bson format of ObjectId from str type
        _id = filtre.get('_id')
        if _id:
            filtre.update({'_id': ObjectId(_id)})

        print 'looking for object to store in database'
        original = self.root_db[collection].find_one(filtre, {'_id': 0})
        if not original:
            raise ValueError("Object not found, check the condition or maybe it's already been proceeded")

        # store object found
        return self.store_object(original, target_storage, target_table)

    def read_multipart(self, part):
        if isinstance(part, str) or isinstance(part, unicode):
            part_info = part
        elif isinstance(part, dict) and part.get('address'):
            part_info = self.storage_manager.read(part.get('storage'),
                                                  part.get('collection'),
                                                  {'address': part.get('address')})
            for subpart in part_info:
                part_info.update({subpart: self.read_multipart(part_info.get(subpart))})
        elif isinstance(part, list):
            part_info = [self.read_multipart(subpart) for subpart in part]
        else:
            raise TypeError('TODO: handle with unrecognized type ' + type(part).__name__)
        return part_info

    def read_subpart(self, part, projection):
        print 'READ INFO: split projection'
        projection = projection.split('.', 1)

        actual_part = None

        if isinstance(part, dict):
            print 'READ INFO: actual part is one instance'
            actual_part = part

        elif isinstance(part, list):
            print 'READ INFO: actual part is a list of instances'
            for subpart in part:
                if subpart.get('collection') == projection[0]:
                    actual_part = subpart
                    break

        else:
            raise TypeError('deal with other types of parts')

        print 'READ INFO: read next part'
        next_part = self.storage_manager.read(actual_part.get('storage'),
                                              actual_part.get('collection'),
                                              {'address': actual_part.get('address')})
        if not next_part:
            raise ValueError('Invalid path')

        if next_part.get('value'):
            next_part = next_part.get('value')

        if len(projection) == 1:
            return self.read_multipart(next_part)

        else:
            next_projection = projection[1]
            return self.read_subpart(next_part, next_projection)

    def read_handler(self, msg):
        storage = msg.get('storage')
        collection = msg.get('collection')
        filtre = msg.get('filtre', {})
        projection = msg.get('projection')

        if not storage or not collection or not filtre:
            raise ValueError('Information not complete')

        # transform _id from str type to ObjectId
        _id = filtre.get('_id')
        if _id:
            filtre.update({'_id': ObjectId(_id)})

        # look up for object from storage
        print 'READ INFO: get root object from storage'
        read_object = self.storage_manager.read(storage, collection, filtre)

        if projection is None:
            # If no demand for projection, get object with its entire content
            for part in read_object:
                read_object.update({part: self.read_multipart(read_object.get(part))})
            return _id, read_object
        else:
            projected_object = {}
            # If there exists projection, look up for only needed part
            for key in projection.split(','):
                projected_object.update({key: {
                    'value': self.read_subpart(read_object.get(key.split('.')[0]), key)
                }})
            return _id, projected_object

    def run(self, redis):
        """
        Run redis message handler
        :param redis:   message bus for all operations
        """
        subscription = redis.pubsub(ignore_subscribe_messages=True)
        subscription.subscribe('read', 'write')
        print 'listening on channels'

        for msg in subscription.listen():
            if msg['channel'] == 'write':
                print 'receive from "write" channel: {}'.format(msg)
                try:
                    print self.write_handler(json.loads(msg['data']))
                except:
                    print 'fail to write into database'

            elif msg['channel'] == 'read':
                print 'receive from "read" channel: {}'.format(msg)
                _id, result = self.read_handler(json.loads(msg['data']))
                redis.publish('read_result', json.dumps({
                    '_id': _id,
                    'result': result
                }))
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


