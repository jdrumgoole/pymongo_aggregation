"""

Author: joe@joedrumgoole.com

"""
import pymongo

from pymongo_aggregation.agg_operation import project, out
from pymongo_aggregation.pipeline import Pipeline

if __name__ == "__main__":
    # location: {
    #     type: "Point",lon lat
    #     coordinates: [-73.856077, 40.848447]
    # }

    point_mapper = {  "_id": 0,
                    "VendorID": 1,
                    "tpep_pickup_datetime": 1,
                    "tpep_dropoff_datetime": 1,
                    "passenger_count": 1,
                    "trip_distance": 1,
                    "pickup_point" : { "type" : "Point",
                                       "coordinates": [ "$pickup_longitude", "$pickup_latitude"]
                                       },
                    "RatecodeID": 1,
                    "pickup_longitude" : 1,
                    "pickup_latitude" : 1,
                    "store_and_fwd_flag": 1,
                    "dropoff_longitude": 1,
                    "dropoff_latitude": 1,
                    "payment_type": 1,
                    "fare_amount": 1,
                    "extra": 1,
                    "mta_tax": 1,
                    "tip_amount": 1,
                    "tolls_amount": 1,
                    "improvement_surcharge": 1,
                    "total_amount": 1 }


    client     = pymongo.MongoClient()
    database   = client["NYC"]
    collection = database[ "taxi"]
    projector = project( point_mapper )
    new_collection = out( "taxi_geo")

    pipeline = Pipeline( projector, new_collection )

    print( "Processing")
    pipeline.aggregate(collection)