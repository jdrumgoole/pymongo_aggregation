#!/bin/sh
mongorestore --gzip --archive=test_agg.zip --nsTo="TEST_AGG.test_groups" --nsFrom="TEST_AGG.test_groups" --drop
