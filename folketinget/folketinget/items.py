# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field
    
class MeetingItem(scrapy.Item):
    meeting_id = Field()
    url = Field()
    date = Field()

class AgendaItem(scrapy.Item):
    meeting_id = Field()
    agenda_item_id = Field()
    title = Field()
    type = Field()
    
class SpeechItem(scrapy.Item):
    meeting_id = Field()
    agenda_item_id = Field()
    speech_item_id = Field()
    time_start = Field()
    time_end = Field()
    speaker_name = Field()
    speaker_party = Field()
    speaker_role = Field()
    speaker_title = Field()
    speech_item_text = Field()
    