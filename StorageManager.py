#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import random


class StorageManager(object):
    def __init__(self):
        self.storages = []
        self._default_storage = None

    def is_db(self, storage_name):
        storage = self.get_storage_instance(storage_name)
        if not storage:
            raise TypeError("Storage doesn't exist")
        return storage.get('type') == 'db'

    def get_databases(self):
        return [{'name': storage.get('name'),
                 'type': 'db'}
                for storage in self.storages
                if storage.get('type') == 'db']

    def get_filesystems(self):
        return [{'name': storage.get('name'),
                 'type': 'fs'}
                for storage in self.storages
                if storage.get('type') == 'fs']

    def get_storage_instance(self, storage_name):
        for storage in self.storages:
            if storage_name == storage.get('name'):
                return storage
        return None

    def get_storage_handler(self, storage_name, io_type='write'):
        storage = self.get_storage_instance(storage_name)
        if not storage:
            raise TypeError("Storage doesn't exist")
        return storage.get('connection'), storage.get(io_type)

    def set_storage_handler(self, storage_name, handler, io_type='write'):
        storage = self.get_storage_instance(storage_name)
        if not storage:
            raise TypeError("Storage doesn't exist")
        storage.update({io_type: handler})

    def add_storage(self, conn, name, storage_type='db', write_handler=None, read_handler=None):
        self.storages.append({
            'name': name,
            'type': storage_type,
            'connection': conn,
            'write': write_handler,
            'read': read_handler
        })

    def get_default_storage(self):
        if not self._default_storage:
            self._default_storage = random.choice(self.storages)
        return self._default_storage

    def set_default_storage(self, storage_name):
        storage = self.get_storage_instance(storage_name)
        self._default_storage = storage if storage else self.get_default_storage()

    def write(self, storage_name, content, placement=None):
        storage_conn, writer = self.get_storage_handler(storage_name, 'write')
        if not writer:
            raise NotImplementedError("Writer handler for storage not defined!")

        return writer(storage_conn, content, placement)

    def read(self, storage_name, placement, filtre):
        storage, reader = self.get_storage_handler(storage_name, 'read')
        if not reader:
            raise NotImplementedError("Reader handler for storage not defined!")
        return reader(storage, placement, filtre)
