#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import re
import random


def get_storage_instance(storage_set, storage_name):
    for inst in storage_set:
        if storage_name == inst.get('name'):
            return inst
    return None


def set_handler(storage_set, storage, io_type, handler):
    for inst in storage_set:
        if storage == inst.get('name'):
            inst.update({io_type: handler})
            return True
    return False


def extract_name(content_type):
    """
    Get field 'name' from content_type string, in the 1st
    :param content_type:
    :return: name
    """
    content_type = (content_type.replace('; ', ';')
                    .split(';'))
    for part in content_type:
        part = part.translate(None, '\r\t\n')
        if re.search('^name', part):
            return part.split('=')[1].replace('"', '')
    return None


class StorageManager(object):
    def __init__(self):
        self._databases = []
        self._filesystems = []
        self._default_storage = None

    def get_databases(self):
        return [{'name': db.get('name'),
                 'type': db.get('type')}
                for db in self._databases]

    def get_filesystems(self):
        return [{'name': fs.get('name'),
                 'type': fs.get('type')}
                for fs in self._filesystems]

    def get_io(self, storage_name, storage_type, io_type):
        if not (storage_type == 'db' or storage_type == 'fs'):
            raise TypeError("Storage Type doesn't exist, try 'fs' or 'db'")

        if not (io_type == 'write' or io_type == 'read'):
            raise TypeError("Storage io_type doesn't exist")

        storage_set = self._databases if storage_type == 'db' else self._filesystems
        inst = get_storage_instance(storage_set, storage_name)

        return inst.get('connection'), inst.get(io_type)

    def set_io_handler(self, storage_name, storage_type, io_type, handler):
        if not (storage_type == 'db' or storage_type == 'fs'):
            raise TypeError("Storage Type doesn't exist, try 'fs' or 'db'")

        if not (io_type == 'write' or io_type == 'read'):
            raise TypeError("Storage io_type doesn't exist")

        storage_set = self._databases if storage_type == 'db' else self._filesystems

        return set_handler(storage_set, storage_name, io_type, handler)

    def write(self, storage_name, content, storage_type='db', placement=None):
        # TODO: read file storage and write to destination
        conn, handler = self.get_io(storage_name, storage_type, 'write')
        if not handler:
            raise NotImplementedError("I/O handlers for storage not defined!")

        return handler(conn, content, placement) if placement else handler(conn, content)

    def add_database(self, db_conn, name, db_type='SQL', write_handler=None):
        self._databases.append({
            'name': name,
            'type': 'db',
            'subtype': db_type,
            'connection': db_conn,
            'write': write_handler
        })

    def add_filesystem(self, fs_conn, name, fs_type='NFS', write_handler=None):
        self._filesystems.append({
            'name': name,
            'type': 'fs',
            'subtype': fs_type,
            'connection': fs_conn,
            'write': write_handler
        })

    def get_default_storage(self, storage_type='db'):
        if not self._default_storage:
            storage_set = self._databases if storage_type == 'db' else self._filesystems
            self._default_storage = random.choice(storage_set)
        return self._default_storage

    def set_default_storage(self, storage_name, storage_type='db'):
        storage_set = self._databases if storage_type == 'db' else self._filesystems
        for storage in storage_set:
            if storage['name'] == storage_name:
                self._default_storage = storage
                return

    def read(self, storage, collection, filtre, storage_type='db'):
        store = self.get_io_handler(storage, storage_type, 'read')
