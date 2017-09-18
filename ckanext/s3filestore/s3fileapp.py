# (c) 2005 Ian Bicking and contributors; written for Paste (http://pythonpaste.org)
# Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
# (c) 2005 Ian Bicking, Clark C. Evans and contributors
# This module is part of the Python Paste Project and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""
This module streams content from a fileobj to the client.

Based upon fileapp.py from python-paste.
"""

import os, time, mimetypes, zipfile, tarfile
from paste.httpexceptions import *
from paste.httpheaders import *
from paste.fileapp import DataApp

BLOCK_SIZE = 4096 * 16

__all__ = ['DataApp', 'FileApp', 'DirectoryApp', 'ArchiveStore']


class S3FileApp(DataApp):
    """
    Returns an application that will send the file-like object
    ``fd``.  Adds a mime type based on ``mimetypes.guess_type()``.
    See DataApp for the arguments beyond ``filename``.
    """

    def __init__(self, s3_object, headers=None, **kwargs):
        self.s3_object = s3_object
        DataApp.__init__(self, None, headers, **kwargs)

    def update(self, force=False):
        self.s3_object.load()
        self.last_modified = time.mktime(self.s3_object.last_modified.timetuple())
        self.content_length = self.s3_object.content_length
        self.content_encoding = self.s3_object.content_encoding
        LAST_MODIFIED.update(self.headers, time=self.last_modified)

    def calculate_etag(self):
        return self.s3_object.e_tag

    def get(self, environ, start_response):
        is_head = environ['REQUEST_METHOD'].upper() == 'HEAD'
        if 'max-age=0' in CACHE_CONTROL(environ).lower():
            self.update(force=True)  # RFC 2616 13.2.6
        else:
            self.update()
        retval = DataApp.get(self, environ, start_response)
        if isinstance(retval, list):
            # cached content, exception, or not-modified
            if is_head:
                return [b'']
            return retval
        (lower, content_length) = retval
        if is_head:
            return [b'']
        return _S3ResponseIter(
            self.s3_object.get(
                Range="bytes=%d-" % (lower)), content_length)


class _S3ResponseIter(object):

    def __init__(self, s3_response, size):
        self.s3_body = s3_response['Body']
        self.size = size

    def __iter__(self):
        return self

    def next(self):
        chunk_size = min([BLOCK_SIZE, self.size])
        data = self.s3_body.read(chunk_size)
        self.size -= chunk_size
        if len(data) == 0:
            raise StopIteration
        return data
    __next__ = next

    def close(self):
        self.s3_body.close()
