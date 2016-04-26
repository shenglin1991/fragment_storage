#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-


def get_method(storage_set, name, io_type):
    for inst in storage_set:
        if name == inst.get('name'):
            return inst.get(io_type)


def set_method(storage_set, name, io_type, method):
    for inst in storage_set:
        if name == inst.get('name'):
            inst.update({io_type: method})


class StorageManager(object):
    def __init__(self):
        self._databases = []
        self._filesystems = []

    def get_databases(self):
        return [{'name': _db.get('name'),
                 'type': _db.get('type')}
                for _db in self._databases]

    def get_filesystems(self):
        return [{'name': _fs.get('name'),
                 'type': _fs.get('type')}
                for _fs in self._filesystems]

    def get_io_method(self, name, stype, io_type):
        if not (stype == 'db' or stype == 'fs'):
            raise TypeError("Storage Type doesn't exist, try 'fs' or 'db'")

        if not (io_type == 'write' or io_type == 'read'):
            raise TypeError("Storage io_type doesn't exist")

        storage_set = self._databases if stype == 'db' else self._filesystems

        return get_method(storage_set, name, io_type)

    def set_io_method(self, name, stype, io_type, method):
        if not (stype == 'db' or stype == 'fs'):
            raise TypeError("Storage Type doesn't exist, try 'fs' or 'db'")

        if not (io_type == 'write' or io_type == 'read'):
            raise TypeError("Storage io_type doesn't exist")

        storage_set = self._databases if stype == 'db' else self._filesystems

        return set_method(storage_set, name, io_type, method)

    def write(self, name, content, stype='db'):
        store = self.get_io_method(name, stype, 'write')
        if not store:
            raise NotImplementedError("I/O Methods for storage not defined!")

        return store(content)

    """
    def read(self, name, condition, stype='db'):
        store = self.get_io_method(name, stype, 'read')
    """

    def add_database(self, db_conn, name, db_type='SQL', write_method=None):
        self._databases.append({
            'name': name,
            'type': db_type,
            'connection': db_conn,
            'write': write_method
        })

    def add_filesystem(self, fs_conn, name, fs_type='NFS', write_method=None):
        self._filesystems.append({
            'name': name,
            'type': fs_type,
            'connection': fs_conn,
            'write': write_method
        })
