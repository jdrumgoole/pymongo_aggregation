import pymongo

client = pymongo.MongoClient()

db = client[ "TEST_AGG"]

col = db[ "groups"]
new_col = db["test_groups"]

distinct_values = col.distinct( "group.id")

count = 0
for i in distinct_values:
    v = col.find_one( { "group.id" : i })
    count = count + 1
    print(count)
    new_col.insert_one(v)
