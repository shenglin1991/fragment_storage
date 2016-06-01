#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import random


class StorageManager(object):
    def __init__(self):
        self.storages = []
        self._default_storage = None

    def get_databases(self):
        return [{'name': db.get('name'),
                 'type': 'db'}
                for db in self.storages
                if db['type'] == 'db']

    def get_filesystems(self):
        return [{'name': fs.get('name'),
                 'type': 'fs'}
                for fs in self.storages
                if fs['type'] == 'fs']

    def get_storage_instance(self, storage_name, storage_type):
        for storage in self.storages:
            if storage_name == storage['name'] and storage_type == storage['type']:
                return storage
        return None

    def get_storage_handler(self, storage_name, storage_type, io_type='write'):
        storage = self.get_storage_instance(storage_name, storage_type)
        if not storage:
            raise TypeError("Storage doesn't exist")
        return storage.get('connection'), storage.get(io_type)

    def set_storage_handler(self, storage_name, handler, storage_type='db', io_type='write'):
        for storage in self.storages:
            if storage_name == storage['name'] and storage_type == storage['type']:
                storage.update({io_type: handler})
                return True
        return False

    def add_database(self, db_conn, name, db_type='SQL', write_handler=None, read_handler=None):
        self.storages.append({
            'name': name,
            'type': 'db',
            'subtype': db_type,
            'connection': db_conn,
            'write': write_handler,
            'read': read_handler
        })

    def add_filesystem(self, fs_conn, name, fs_type='NFS', write_handler=None, read_handler=None):
        self.storages.append({
            'name': name,
            'type': 'fs',
            'subtype': fs_type,
            'connection': fs_conn,
            'write': write_handler,
            'read': read_handler
        })

    def get_default_storage(self, storage_type='db'):
        if not self._default_storage:
            self._default_storage = random.choice([storage for storage in self.storages
                                                   if storage['type'] == storage_type])
        return self._default_storage

    def set_default_storage(self, storage_name, storage_type=None):
        for storage in self.storages:
            if storage['name'] == storage_name and (not storage_type or storage['type'] == storage_type):
                self._default_storage = storage
                return

    def write(self, storage_name, content, storage_type='db', placement=None):
        storage_conn, writer = self.get_storage_handler(storage_name, storage_type, 'write')
        if not writer:
            raise NotImplementedError("Writer handler for storage not defined!")

        return writer(storage_conn, content, placement)

    def read(self, storage_name, placement, filtre, storage_type='db'):
        storage, reader = self.get_storage_handler(storage_name, storage_type, 'read')
        if not reader:
            raise NotImplementedError("Reader handler for storage not defined!")
        return reader(storage, placement, filtre)
