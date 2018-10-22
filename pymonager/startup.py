import pymongo
from pymonager.pipeline import Pipeline, CursorIterator, project_doc
from pymonager.agg import match, project

client = pymongo.MongoClient()
db = client["LONDON_MUG"]
attendees = db["attendees"]
p = Pipeline(attendees, match({"attendee.rsvp.response" : "yes"}))
it = CursorIterator(p.aggregate())
a=attendees.find_one()
d=project_doc(a, "attendee", "member")

