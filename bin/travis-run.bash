#!/bin/bash
set -e

pytest --ckan-ini=subdir/test.ini --cov=ckanext.s3filestore --disable-warnings ckanext/s3filestore/tests