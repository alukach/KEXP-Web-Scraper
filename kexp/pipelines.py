# -*- coding: utf-8 -*-
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/topics/item-pipeline.html

#from scrapy.exceptions import DropItem

# https://github.com/darkrho/scrapy-googledir-mysql/blob/master/googledir/pipelines.py#L23

import sys
from datetime import datetime
import unicodedata
import psycopg2
import items
from scrapy import log
from scrapy.project import crawler
from scrapy.exceptions import DropItem
from kexp import settings


# GOALS
# --------------------
# 1) when spider starts (open_spider()), check for current db, and consequently open existing DB or create new DB
# Notes: http://doc.scrapy.org/topics/item-pipeline.html#topics-item-pipeline
# 2) convert item['time'] from string (ex. 10:57PM) to time datatype (ex. 22:57:00)
#
# Question: Better (faster?) to have a unique cursor for each item or one cursor?

log.msg("Pipeline started.", level=log.DEBUG)

class KexpPipeline(object):
    def __init__(self): # Connect to database on initialization (we want this to run once per pipeline)
        self.drops = counter('Dropped Songs')
        self.inserts = counter('Inserted Songs')

        databaseName = settings.databaseName
        databaseUser = settings.databaseUser
        databasePswd = settings.databasePswd
        databaseHost = settings.databaseHost
        self.databaseTable = settings.KEXPdatabaseTable
        conn_string = "dbname='%s' user='%s' password='%s' host='%s'" % (databaseName, databaseUser, databasePswd, databaseHost)
        try:
            self.conn = psycopg2.connect(conn_string)
            log.msg("Successfully connected to database \"%s\''." % (databaseName), level=log.INFO)
            self.cur = self.conn.cursor()
        except:
            # Get the most recent exception
            exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
            log.msg("Failed to connect to database (%s).  Shutting down spider." % exceptionValue, level=log.ERROR)
            crawler._signal_shutdown(9,0) #Kills the Spider if connection fails.

    def process_item(self, item, spider):
        log.msg("Processing \"%s - %s\"(playid: %s, playdatetime: %s)." % (item['artist'], item['songtitle'], item['playid'], item['playdatetime']), level=log.DEBUG)

        if settings.DROPCOUNT_CONTROL != 0 and self.drops.in_row > (settings.DROPCOUNT_CONTROL - 1): # #DebugTool: Limit number of times an item can be dropped.
            print "*************************************"
            print "Drop limit met.  Goodbye."
            print "*************************************"
            crawler._signal_shutdown(9,0) #Kills the Spider
        elif self.check_duplicate(self.databaseTable, item, ['playid', 'station']):
            log.msg("Duplicate item found.  Dropping \"%s - %s\"(playid: %s, playdatetime: %s)." % (item['artist'], item['songtitle'], item['playid'], item['playdatetime']), level=log.ERROR)
            print "Duplicate item found.  Dropping \"%s - %s\"(%s)." % (item['artist'], item['songtitle'], self.drops.in_row)
            raise DropItem("Item already exists in db.")
        else:
            log.msg("No duplicate found.  Inserting \"%s - %s\"(playid: %s, playdatetime: %s)." % (item['artist'], item['songtitle'], item['playid'], item['playdatetime']), level=log.DEBUG)
            self.insert_item(self.databaseTable, item)
        return item

    def check_duplicate(self, table_name, item, criteria_list):
        instr = 'SELECT * FROM {table} WHERE {criteria};'.format( # Assemble query string, part 1 (the instructions)
            table = table_name,
            criteria = " AND ".join(c + " = %s" for c in criteria_list)
            )
        data = [item[c] for c in criteria_list] # Create query string, part2 (the data being searched for)
        self.cur.execute(instr, data) # Combine the instructions and data
        rows = self.cur.fetchall() # Fetch results
        self.conn.rollback() #close connection, do not save any changes
        if rows: # If matching rows were found (ie duplicate value), return True
            self.drops.in_row += 1
            self.drops.total += 1
            return True
        else: # Else, if no matching rows were found (ie not duplicate), return False
            self.drops.in_row = 0
            return False

    def insert_item(self, table_name, item):
        print_item(item)
        keys = item.keys()
        data = [item[k] for k in keys] # make a list of each key
        instr = 'INSERT INTO {table} ({keys}) VALUES ({placeholders});'.format( # Assemble query string
            table = table_name, #table to insert into, provided as method's argument
            keys = ", ".join(keys), #iterate through keys, seperated by comma
            placeholders = ", ".join("%s" for d in data) #create a %s for each key
            )
        self.cur.execute(instr, data) # Combine the instructions and data
        self.conn.commit()
        log.msg("Successfully inserted \"%s - %s\" into database." % (item['artist'], item['songtitle']), level=log.DEBUG)
        print "Successfully inserted \"%s: %s - %s\" into database table \"%s\"." % (item['playid'], item['artist'], item['songtitle'], self.databaseTable)
        self.inserts += 1

    def print_item(self, item):
        print "~~~~~~~~~Item~~~~~~~~~"
        for key, value in item.iteritems():
            print '{0}: {1}'.format(key, value)
        return
    def get_counts(self):
        return "FUCK"
class counter:
    def __init__(self, name):
        self.name = name
        self.in_row = 0
        self.total = 0
    def __string__(self):
        message = self.name + " in a row: " + self.in_row
        message = message + self.name + " in total: " + self.total
        return message
