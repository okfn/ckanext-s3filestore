from nose.tools import (
                        assert_equal,
                        assert_true,
                        assert_false
                        )

from ckan.tests import helpers

import ckanext.s3filestore.model as model


class TestFilestoreUrlMapModel(object):

    def setup(self):
        helpers.reset_db()

    def test_get_or_create_exists(self):
        '''For an existing object, use get_or_create to retrieve it.'''
        # create a FilestoreUrlMap object
        map_obj = model.FilestoreUrlMap.create(id="some-id",
                                               url="http://www.example.com")
        # get it
        retrieved_obj, created = \
            model.FilestoreUrlMap.get_or_create(id="some-id")

        assert_equal(map_obj, retrieved_obj.as_dict())
        assert_false(created)

    def test_get_or_create_non_exists(self):
        '''For a non existing object, use get_or_create to create it.'''
        # get it
        retrieved_obj, created = \
            model.FilestoreUrlMap.get_or_create(id="some-id")

        assert_true(created)
