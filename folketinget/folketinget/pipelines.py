# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import csv

class FolketingetPipeline:
    def process_item(self, item, spider):
        return item


# class SequentialCSVExportPipeline:
#     def __init__(self):
#         self.file = None
#         self.exporter = None

#     def open_spider(self, spider):
#         self.file = open('data_sample.csv', 'w', newline='', encoding='utf-8')
#         self.exporter = csv.DictWriter(self.file, fieldnames=['meeting_id', 'agenda_item_id', 'question_item_id', 'speech_item_id', 'url', 'speech_item_text'])
#         self.exporter.writeheader()

#     def process_item(self, item, spider):
#         self.exporter.writerow(dict(item))
#         return item

#     def close_spider(self, spider):
#         if self.file:
#             self.file.close()


    
