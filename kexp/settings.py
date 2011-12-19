# Scrapy settings for kexp project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/topics/settings.html
#
import datetime

BOT_NAME = 'kexp'
BOT_VERSION = '1.0'

SPIDER_MODULES = ['kexp.spiders']
NEWSPIDER_MODULE = 'kexp.spiders'
DEFAULT_ITEM_CLASS = 'kexp.items.KexpItem'
USER_AGENT = '%s/%s' % (BOT_NAME, BOT_VERSION)

RETRY_CONTROL = 3       # Number of times to try to connect to a URL before it's skipped over. Suggested: 3
FAIL_CONTROL = 10       # Number of URLs allowed to fail in succession before Spider is stopped. Suggested; 10
SCRAPECOUNT_CONTROL = 0 # Number of URLs to scrape. (for debugging, 0 to disable)
DROPCOUNT_CONTROL = 11   # Number of duplicate/dropped items to see before spider is stopped. (0 to disable)

SEARCH_START_TIME = datetime.datetime(2008, 1, 4, 14) # DEBUG MODE: Manually set start date. (year, month, day, hour)
#SEARCH_START_TIME = datetime.datetime.now()-datetime.timedelta(hours=8) # SERVER MODE: minus 7 hrs to adjust to PST from server's time (GMT) and then minus 1 hr
#SEARCH_START_TIME = datetime.datetime.now()-datetime.timedelta(hours=1) # LOCAL MODE: minus 1 hr

ITEM_PIPELINES = [
    'kexp.pipelines.KexpPipeline',
]

# Database Settings
# --------------------
databaseName = 'kexp'
databaseUser = 'kexp_user'
databasePswd = 'kexp_pass'
databaseHost = 'localhost'
KEXPdatabaseTable = 'kexp'

#   Notes:
#   a) 2011, 3, 13, 2 : Error where there is no playlist.
#   b) 2002, 1, 1, 0 : Last visible playlist on website (although the scraper continues to return data).

# Command Scratch Pad
# scrapy crawl kexp --logfile=logfile.log --loglevel=ERROR
# scrapy crawl kexp --logfile=logfile2.log --loglevel=ERROR --set FEED_URI=items2.json --set FEED_FORMAT=json
# scrapy crawl kexp --set FEED_URI=test.xml --set FEED_FORMAT=xml
# scrapy crawl kexp --logfile=test_logfile.log --loglevel=ERROR --set FEED_URI=test.xml --set FEED_FORMAT=xml