# -*- coding: utf-8 -*-
############################################################################
#
# This is a spider to grab KEXP's playlist data.
#
# author: Anthony Lukach
# date: July 22, 2011
#
# TO DO: Impelement logging (see line 57)
#
############################################################################

import datetime
from scrapy import log
from scrapy.item import Item
from scrapy.http import Request
from scrapy.spider import BaseSpider
from scrapy.selector import HtmlXPathSelector
from scrapy.contrib.loader import XPathItemLoader
from scrapy.contrib.loader.processor import TakeFirst, Join, Compose, MapCompose
from kexp.items import KexpItem #grab item list from ../kexp/items.py
from kexp import settings # needed for controls
from kexp import pipelines #needed to see Drop counter for stats()

# Counters
attempts_count = 0    # Counter used to keep track of how URLs looked at for scraping.
repeat_fail_count = 0   # Counter used to keep track of how many times, in a row, succeeding URLs fail.
total_fail_count = 0  # Counter used to keep track of how many total URL failurs occur.
success_count = 0     # Counter used to keep track of how many URLs were successfully scraped.
start_time = datetime.datetime.now()

searchURL = ''
name = "kexp_Scraper"
logfilename = "%s_%s_%s.log" % ( str(datetime.date.today()), str(datetime.datetime.now().strftime("%Hh%Mm%Ss")), name )
log.start()
searchtime = settings.SEARCH_START_TIME

#
# Does some minor formatting to items before being sent to Pipeline
# -------------------------------------
class KexpLoader(XPathItemLoader):
    def make_playDateTime(preformat_playdatetime): # Combines date (from given URL) and play time into single datetime value.
        format = "http://kexp.org/playlist/playlist.aspx?t=1&year=%Y&month=%m&day=%d&%I:%M%p" #defines the format of the string, declaring where the Year, Month, Day, Hour, Minute, AM/PM can be found
        try:
            playDateTime = datetime.datetime.strptime( preformat_playdatetime, format )
            return playDateTime
        except:
            log.msg("Unable to create playDateTime for song (%s)." % (preformat_playdatetime), level=log.ERROR)
            return None
            
    def strip_spaces(x):
        return x.replace(" ",'')
    def blank_check(x):
        if x:
            return x
        else:
            print "Found blank item."
            return ''
            
    default_item_class = KexpItem
    default_output_processor = Compose(Join()) # Converts item from list of unicode text to just unicode text
    artist_out = Compose(Join(), blank_check)
    songtitle_out = Compose(Join(), blank_check)
    playdatetime_out = Compose(Join(), strip_spaces, make_playDateTime)

#
# Assembles the URL based on searchtime
# -------------------------------------
def generateURL():
    global searchURL
    global attempts_count

    if settings.SCRAPECOUNT_CONTROL != 0 and attempts_count > (settings.SCRAPECOUNT_CONTROL - 1): # #DebugTool: Limit number of times it can run.  To enable, set to " > [limit#]". To disable, set to " < 0".
        print "*************************************"
        print "Scrape limit met.  Goodbye."
        print "*************************************"
        statistics('Final')
        return

    attempts_count = attempts_count + 1
    searchURL = "http://kexp.org/playlist/playlist.aspx?t=1&year=%s&month=%s&day=%s&hour=%s" % (
        str(searchtime.year),
        str(searchtime.month),
        str(searchtime.day),
        str(searchtime.hour)
      )
    print "NOTE: Attempt #%s: searchURL set to %s" % (attempts_count, searchURL) #DebugNote
    return searchURL

#
# Returns the next URL, created by incrementing the searchtime by one less hour.
# -------------------------------------
def nextURL():
    global searchtime
    global searchURL
    searchtime = searchtime - datetime.timedelta(hours=1)
    searchURL = generateURL()
    return searchURL

#
# Create Spider
# -------------------------------------
class KexpSpider(BaseSpider) :
    name = "kexp"
    allowed_domains = ["kexp.org"]
    start_url = generateURL()
    
    def start_requests(self):
        # NOTE: This method is ONLY CALLED ONCE by Scrapy (to kick things off).
        # Get the first url to crawl and return a Request object
        # This will be parsed to self.parse which will continue
        # the process of parsing all the other generated URLs

        global searchURL        #cur.execute()

        url = searchURL
        firstRequest = Request(str(searchURL), dont_filter=True)
        return [firstRequest]

    def parse(self, response) :
        global repeat_fail_count
        global total_fail_count
        retry_count = 0

        hxs = HtmlXPathSelector(response)
        songs = hxs.select('//dl[@class="play"]') #grabs songs (finds each section encased by <dl class="play"> )

        # If an empty songs is returned, the attempt is made again:
        while not songs and retry_count < settings.RETRY_CONTROL:
            retry_count = retry_count + 1
            print "Playlist not found (%s).  Retry: %s." % ( str(searchURL), str(retry_count) )
            songs = hxs.select('//dl[@class="play"]') #grabs songs (finds each section encased by <dl class="play"> )

        # If after 3 tries of a single URL, still no songs loaded, and it hasn't yet tried 3 different URLs, try the next URL
        if not songs and retry_count == settings.RETRY_CONTROL:
            print '----------------------------------------------------------'
            print "Playlist not found (%s).  Trying next song." % ( str(searchURL) )
            repeat_fail_count = repeat_fail_count + 1
            total_fail_count = total_fail_count +1
            print "FAIL COUNT: %s\n" % ( repeat_fail_count )

            if repeat_fail_count < settings.FAIL_CONTROL:
                log.msg("Failed to retrieve %s" % response.url, level=log.ERROR)
                yield Request( nextURL() )
            else:
                log.msg("Failed to retrieve %s. %s URLs failed in succession.  Stopping Spider." % (response.url, repeat_fail_count), level=log.ERROR)
                print '----------------------------------------------------------'
                print "%s URLs failed in succession.  Stopping Spider.  Goodbye." % repeat_fail_count
                print '----------------------------------------------------------'
                statistics('Final')

                return

        # If a song was returned:
        else:
            repeat_fail_count = 0 #reset global repeat_fail_count
            log.msg("Scraping '%s'" % ( response.url ), level=log.INFO )

            for song in songs: #iterates through each <dl class="play">
                kl = KexpLoader(selector=song)

                kl.add_xpath('dj','//div[@id="show"]/s/d/text()')
                kl.add_xpath('showtitle','//div[@id="show"]/s/n/text()')
                kl.add_xpath('showgenre','//div[@id="show"]/s/nf/text()')
                kl.add_xpath('showblock','//div[@id="show"]/s/t/text()')
                kl.add_xpath('playid','dd[@class="playid"]/text()')
                #kl.add_xpath('time','dd[@class="song"]/dl/dd[@class="time"]/text()') #playdatetime makes this redundant

                kl.add_xpath('artist','dd[@class="song"]/dl/dd[@class="artist"]/text()')
                kl.add_xpath('songtitle','dd[@class="song"]/dl/dd[@class="songtitle"]/text()')
                # kl.add_value('songtitle', '') #Another option to put blank songs through the Item Exporter
                kl.add_xpath('album','dd[@class="song"]/dl/dd[@class="album"]/text()')
                kl.add_xpath('releaseyear','dd[@class="song"]/dl/dd[@class="releaseyear"]/text()')
                kl.add_xpath('label','dd[@class="song"]/dl/dd[@class="label"]/text()')
                kl.add_xpath('djcomments','dd[@class="djcomments"]/text()')
                kl.add_value('station', 'kexp')

                kl.add_value('datetime_scraped', str(datetime.datetime.now()) )
                kl.add_value('source_url', searchURL )
                kl.add_xpath('source_title','//title/text()', re='\r\n\t(.*)\r\n')

                kl.add_value('playdatetime', searchURL, re='(.*)hour=')
                kl.add_xpath('playdatetime', 'dd[@class="song"]/dl/dd[@class="time"]/text()')

		log.msg("Successfully retrieved %s" % response.url, level=log.INFO)

                kl.get_output_value('songtitle') #this is the only way I can get a blank 'songtitle' to go through the Item Loader
                kl.get_output_value('artist') #this is the only way I can get a blank 'songtitle' to go through the Item Loader

                yield kl.load_item() #return individual scraped song

            global success_count
            success_count = success_count + 1
            statistics('Current')
            yield Request(str(nextURL()))

# End statistics

def statistics(title):
    global attempts_count
    global total_fail_count
    global success_count
    global start_time
    now = datetime.datetime.now()
    run_time = now - start_time
    statsprintout = "\n%s Scrape Statistics:\n----------------------------------------------------------" % title
    statsprintout = statsprintout + "\n"
    statsprintout = statsprintout + "Attempted Scrapes: %s || " % ( str(attempts_count) )
    statsprintout = statsprintout + "Successful Scrapes: %s || " % ( str(success_count) )
    statsprintout = statsprintout + "Failed Scrapes: %s" % ( str(total_fail_count) ) #DebugNote
    statsprintout = statsprintout + "\n"
    statsprintout = statsprintout + "Start Time:  %s || " % ( start_time.strftime("%H:%M:%S") )
    statsprintout = statsprintout + "End Time: %s     || " %  ( now.strftime("%H:%M:%S") )
    statsprintout = statsprintout + "Run Time: %s" % ( str(run_time) )
    statsprintout = statsprintout + "\n"
    statsprintout = statsprintout + "AttemptedScrapes/sec:    %.2f || " % ( attempts_count / run_time.total_seconds() )
    statsprintout = statsprintout + "SuccessfulScrapes/sec: %.2f" % ( success_count / run_time.total_seconds() )
    statsprintout = statsprintout + "\n"
    statsprintout = statsprintout + "AttemptedScrapes/min:  %.2f || " % ( (attempts_count / run_time.total_seconds())*60 )
    statsprintout = statsprintout + "SuccessfulScrapes/min: %.2f" % ( (success_count / run_time.total_seconds())*60 )
    statsprintout = statsprintout + "\n"
    statsprintout = statsprintout + "AttemptedScrapes/hr:  %.2f || " % ( (attempts_count / run_time.total_seconds())*3600 )
    statsprintout = statsprintout + "SuccessfulScrapes/hr: %.2f" % ( (success_count / run_time.total_seconds())*3600 )
    statsprintout = statsprintout + "\n"
    statsprintout = statsprintout + "----------------------------------------------------------\n"

    print statsprintout
    hi = pipelines.KexpPipeline.drops
