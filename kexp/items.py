# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/topics/items.html

from scrapy.item import Item, Field

class KexpItem(Item):

    #Song Details
    playid = Field() # Unique identifier given by KEXP to each play
    time = Field() # Time of the day when the song was played
    artist = Field() # Recording artist of the song
    songtitle = Field() # Title of the song
    album = Field() # Album that the song is on
    releaseyear = Field() # Release year of song's album
    label = Field() # Recording label of the song
    djcomments = Field() # Optional comments entered in by DJ

    #DJ/Show Details        
    dj = Field() # DJ who played the song
    showtitle = Field() # Title of the show during which the song was played
    showgenre = Field() # Genre of the show during which the song was played
    showblock = Field() # Time span of the show during which the song was played

    #Meta Details
    station = Field() # Station that played the song
    datetime_scraped = Field() # Date and time when the scrape occured
    source_url = Field() # URL from which the scrape was taken
    source_title = Field() # Title of page from which the scrape was taken
    
    #Created in Pipeline
    playdatetime = Field() # Single datetime value created from source_url and time fields
    
    pass
