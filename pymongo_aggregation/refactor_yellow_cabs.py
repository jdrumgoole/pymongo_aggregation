"""

Author: joe@joedrumgoole.com

"""
import pymongo
import pprint

import argparse

from pymongo_aggregation.aggoperation import project, limit, out
from pymongo_aggregation.pipeline import Pipeline

if __name__ == "__main__":
    # location: {
    #     type: "Point",lon lat
    #     coordinates: [-73.856077, 40.848447]
    # }


    parser=argparse.ArgumentParser()
    parser.add_argument("--host", default="mongodb://localhost:27017" , help="MongoDB URI [default: %(default)s]")
    parser.add_argument("--limit", type=int, help="Limit number of records processed")
    parser.add_argument("--database", type=str, default="test", help="Database to use: [default: %(default)s]" )
    parser.add_argument("--collection", type=str, default="test", help="Collection to use: [default: %(default)s]" )
    parser.add_argument("--out", type=str, default="yellow_cabs", help="Default output collection [default: %(default)s]")

    args = parser.parse_args()

    client = pymongo.MongoClient( host=args.host)
    database   = client[args.database]
    collection = database[ args.collection]
    print('Using input database    : {}'.format(args.database))
    print('Using input collection  : {}'.format(args.collection))
    print('Using output collection : {}'.format(args.out))

    point_mapper = {  "_id": 0,
                    "VendorID": 1,
                    "tpep_pickup_datetime": 1,
                    "tpep_dropoff_datetime": 1,
                    "passenger_count": 1,
                    "trip_distance": 1,
                    "pickup_point" : { "type" : "Point",
                                       "coordinates": [ "$pickup_longitude", "$pickup_latitude"]
                                       },
                    "dropoff_point": { "type": "Point",
                                     "coordinates": ["$dropoff_longitude", "$dropoff_latitude"]
                                    },

                    "RatecodeID": 1,
                    "store_and_fwd_flag": 1,
                    "payment_type": 1,
                    "fare_amount": 1,
                    "extra": 1,
                    "mta_tax": 1,
                    "tip_amount": 1,
                    "tolls_amount": 1,
                    "improvement_surcharge": 1,
                    "total_amount": 1 }

    projector = project(point_mapper)
    new_collection = out(args.out)

    if args.limit and (args.limit > 0):
        pipeline = Pipeline(projector, limit(args.limit), new_collection)
    else:
        pipeline = Pipeline(projector, new_collection)

    print( "Processing")
    pprint.pprint(pipeline)

    pipeline.aggregate(collection)
