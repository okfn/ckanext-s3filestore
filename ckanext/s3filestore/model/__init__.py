from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import types
from sqlalchemy.orm.exc import NoResultFound

from ckan.model.domain_object import DomainObject
from ckan.model.meta import metadata, mapper, Session

import logging
log = logging.getLogger(__name__)


filestore_url_map_table = None


def model_setup():
    # setup filestore_url_map_table
    if filestore_url_map_table is None:
        define_filestore_url_map_table()
        log.debug('FilestoreUrlMap table defined in memory')

    if not filestore_url_map_table.exists():
        filestore_url_map_table.create()
        log.debug('FilestoreUrlMap table created')
    else:
        log.debug('FilestoreUrlMap table already exists')


class BaseModel(DomainObject):
    @classmethod
    def filter(cls, **kwargs):
        return Session.query(cls).filter_by(**kwargs)

    @classmethod
    def exists(cls, **kwargs):
        if cls.filter(**kwargs).first():
            return True
        else:
            return False

    @classmethod
    def get(cls, **kwargs):
        instance = cls.filter(**kwargs).first()
        return instance

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        Session.add(instance)
        Session.commit()
        return instance.as_dict()

    @classmethod
    def get_or_create(cls, **kwargs):
        '''Get an existing object, or create one, based on kwargs. returns a
        tuple: (obj, created).'''
        try:
            return cls.filter(**kwargs).one(), False
        except NoResultFound:
            return cls(**kwargs), True


class FilestoreUrlMap(BaseModel):
    pass


def define_filestore_url_map_table():
    global filestore_url_map_table

    filestore_url_map_table = Table('filestore_url_map', metadata,
                                    Column('id', types.UnicodeText,
                                           primary_key=True),
                                    Column('url', types.UnicodeText,
                                           nullable=True),
                                    )

    mapper(FilestoreUrlMap, filestore_url_map_table)
