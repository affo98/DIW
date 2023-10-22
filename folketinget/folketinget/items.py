# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class FolketingetItem(scrapy.Item):
    meeting_id = Field()
    agenda_item_id = Field()
    question_item_id= Field()
    speech_item_id = Field()
    url = Field()
    #title = Field()
    #date = Field()
    #time = Field()
    #day = Field()
    speech_item_text = Field()
    
