#!/bin/sh
mongodump --db TEST_AGG --collection test_groups --archive=test_agg.zip --gzip
