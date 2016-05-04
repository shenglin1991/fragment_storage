#!/usr/bin/python2.7
# vim:fileencoding=utf8
# -*- coding: utf-8 -*-

import json


class LocalFS:
    def __init__(self, pathname=None):
        self.path = pathname if pathname else './'

    def write(self, conn, content, placement='default', mode='wb'):
        if isinstance(content, dict):
            content = json.dumps(content)

        with open(self.path + placement, mode) as fp:
            fp.write(content)
        return self.path + placement

    def read(self, placement='default', mode='rb'):
        with open(self.path + placement, mode) as fp:
            return fp.read()

    def __str__(self):
        return 'LocalFS'


def file_storage(conf=None):
    return (conf or {}).get('__FILE_STORAGE__', LocalFS())
