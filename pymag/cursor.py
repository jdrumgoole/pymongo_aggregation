import contextlib
import csv
import pprint
import sys
from datetime import datetime
from nesteddict import NestedDict


import pymongo


class CursorFormatter(object):
    '''
    Output a set of cursor elements by iterating over then.

    If root is a file name output the content to that file.
    '''

    def __init__(self, cursor, filename="", formatter="json", results=None):
        '''
        Data from cursor
        output to <filename>suffix.ext.
        '''

        self._results = []
        self._cursor = cursor

        if (isinstance(cursor, pymongo.cursor.Cursor) or
                isinstance(cursor, pymongo.command_cursor.CommandCursor)):
            self._format = formatter
            self._filename = filename
            if results:
                self._results = results
        else:
            raise ValueError("aggregate argument to CursorFormatter is not of class pymongo cursor")

    def results(self):
        return self._results

    @contextlib.contextmanager
    def _smart_open(self, filename=None):
        if filename and filename != '-':
            fh = open(filename, 'w')
        else:
            fh = sys.stdout

        try:
            yield fh
        finally:
            if fh is not sys.stdout:
                fh.close()

    @staticmethod
    def dateMapField(doc, field, time_format=None):
        '''
        Given a field that contains a datetime we want it to be output as a string otherwise
        pprint and other functions will abondon ship when they meet BSON time objects
        '''

        if time_format is None:
            time_format = "%d-%b-%Y %H:%M"
        d = NestedDict(doc)
        if field in d:
            value = d[field]
            if isinstance(value, datetime):
                d[field] = value.strftime(time_format)
            else:
                d[field] = datetime.fromtimestamp(value/1000)

        return dict(d)

    @staticmethod
    def fieldMapper(doc, fields):
        """
        Take 'doc' and create a new doc using only keys from the 'fields' list.
        Supports referencing fields using dotted notation "a.b.c" so we can parse
        nested fields the way MongoDB does.
        """

        if fields is None or len(fields) == 0:
            return doc

        new_doc = NestedDict()
        old_doc = NestedDict(doc)

        for i in fields:
            if i in old_doc:
                # print( "doc: %s" % doc )
                # print( "i: %s" %i )
                new_doc[i] = old_doc[i]
        return dict(new_doc)

    @staticmethod
    def dateMapper(doc, date_map, time_format=None):
        '''
        For all the fields in "datemap" find that key in doc and map the datetime object to
        a strftime string. This pprint and others will print out readable datetimes.
        '''
        if date_map:
            for i in date_map:
                if isinstance(i, datetime):
                    CursorFormatter.dateMapField(doc, i, time_format=time_format)
        return doc

    def printCSVCursor(self, c, fieldnames, datemap, time_format=None):
        '''
        Output CSV format. items are separated by commas. We only output the fields listed
        in the 'fieldnames'. We datemap fields listed in 'datemap'. If a datemap listed field
        is not a datetime object we will thow an exception.
        '''

        with self._smart_open(self._filename) as output:
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            writer.writeheader()
            count = 0
            for i in c:
                self._results.append(i)
                count = count + 1
                d = CursorFormatter.fieldMapper(i, fieldnames)
                d = CursorFormatter.dateMapper(d, datemap, time_format)
                writer.writerow(d)

        return count

    def printJSONCursor(self, c, fieldnames, datemap, time_format=None):
        """

        Output plan json objects.

        :param c: collection
        :param fieldnames: fieldnames to include in output
        :param datemap: fieldnames to map dates to date strings
        :param time_format: field names to map to a specific time format
        :return:
        """

        count = 0

        with self._smart_open(self._filename) as output:
            for i in c:
                # print( "processing: %s" % i )
                # print( "fieldnames: %s" % fieldnames )
                self._results.append(i)
                d = CursorFormatter.fieldMapper(i, fieldnames)
                # print( "processing fieldmapper: %s" % d )
                d = CursorFormatter.dateMapper(d, datemap, time_format)
                pprint.pprint(d, output)
                count = count + 1

        return count

    def printCursor(self, c, fieldnames=None, datemap=None, time_format=None):
        '''
        Output a cursor to a filename or stdout if filename is "-".
        fmt defines whether we output CSV or JSON.
        '''

        if self._format == 'csv':
            count = self.printCSVCursor(c, fieldnames, datemap, time_format)
        else:
            count = self.printJSONCursor(c, fieldnames, datemap, time_format)

        return count

    def output(self, fieldNames=None, datemap=None, time_format=None, aggregate=True):
        '''
        Output all fields using the fieldNames list. for fields in the list datemap indicates the field must
        be date
        '''


        count = self.printCursor(self._cursor, fieldNames, datemap, time_format)
