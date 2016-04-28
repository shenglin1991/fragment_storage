#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import random


def get_storage_instance(storage_set, storage):
    for inst in storage_set:
        if storage == inst.get('name'):
            return inst


def set_handler(storage_set, storage, io_type, handler):
    for inst in storage_set:
        if storage == inst.get('name'):
            inst.update({io_type: handler})


class StorageManager(object):
    def __init__(self):
        self._databases = []
        self._filesystems = []

    def get_databases(self):
        return [{'name': db.get('name'),
                 'type': db.get('type')}
                for db in self._databases]

    def get_filesystems(self):
        return [{'name': fs.get('name'),
                 'type': fs.get('type')}
                for fs in self._filesystems]

    def get_io(self, storage, storage_type, io_type):
        if not (storage_type == 'db' or storage_type == 'fs'):
            raise TypeError("Storage Type doesn't exist, try 'fs' or 'db'")

        if not (io_type == 'write' or io_type == 'read'):
            raise TypeError("Storage io_type doesn't exist")

        storage_set = self._databases if storage_type == 'db' else self._filesystems
        inst = get_storage_instance(storage_set, storage)

        return inst.get('connection'), inst.get(io_type)

    def set_io_handler(self, storage, storage_type, io_type, handler):
        if not (storage_type == 'db' or storage_type == 'fs'):
            raise TypeError("Storage Type doesn't exist, try 'fs' or 'db'")

        if not (io_type == 'write' or io_type == 'read'):
            raise TypeError("Storage io_type doesn't exist")

        storage_set = self._databases if storage_type == 'db' else self._filesystems

        return set_handler(storage_set, storage, io_type, handler)

    def write(self, storage, content, storage_type='db'):
        conn, handler = self.get_io(storage, storage_type, 'write')
        if not handler:
            raise NotImplementedError("I/O handlers for storage not defined!")

        return handler(conn, content)

    """
    def read(self, name, condition, storage_type='db'):
        store = self.get_io_handler(name, storage_type, 'read')
    """

    def add_database(self, db_conn, name, db_type='SQL', write_handler=None):
        self._databases.append({
            'name': name,
            'type': db_type,
            'connection': db_conn,
            'write': write_handler
        })

    def add_filesystem(self, fs_conn, name, fs_type='NFS', write_handler=None):
        self._filesystems.append({
            'name': name,
            'type': fs_type,
            'connection': fs_conn,
            'write': write_handler
        })

    def get_default_storage(self, storage_type='db'):
        storage_set = self._databases if storage_type == 'db' else self._filesystems
        return random.choice(storage_set)
