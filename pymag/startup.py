import pymongo
from pymag.pipeline import Pipeline, CursorIterator, project_doc
from pymag.agg import match, group, sort, out
from dateutil.parser import parse


client = pymongo.MongoClient()
db = client["LONDON_MUG"]
attendees = db["attendees"]
matcher = match({"attendee.rsvp.response": "yes",
                "attendee.member.name": {"$ne" : "Former member"}})
grouper = group({"_id": "$attendee.member.name",
                 "event_count": {"$sum": 1}})
range_match = match(match.time_range_query("event.time",
                                           parse("1-Jan-2016"),
                                           parse("22-Oct-2018")))
sorter = sort(("event_count", pymongo.DESCENDING))
range_data = Pipeline(attendees, matcher, range_match)
limit_match= match({"event_count": {"$gte": 2}})
outer=out("masters")
final = Pipeline(attendees, matcher, range_match, grouper, sorter, limit_match, outer)
rd = CursorIterator(range_data.aggregate())
fd = CursorIterator(final.aggregate())
a=attendees.find_one()
d=project_doc(a, "attendee", "member")

