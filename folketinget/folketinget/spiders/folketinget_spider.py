from pathlib import Path

import scrapy


class FolketingetSpider(scrapy.Spider):
    name = "folketinget"
    
    def __init__(self, *args, **kwargs):
        super(FolketingetSpider, self).__init__(*args, **kwargs)
        self.htm_files = []

    def start_requests(self):
        
        urls = []
        for i in range(1,12):
            print(i)
            base_url = f"https://www.ft.dk/da/dokumenter/dokumentlister/referater?startDate=20041005&endDate=20231001&totalNumberOfRecords=2088&pageSize=200&pageNumber={i}"
            urls.append(base_url)
            
        
        for url in urls:
            #print("\n \n \n \nPARSING")
            print(f'URL REQUEST : {url}')
            yield scrapy.Request(url=url, callback=self.parse)
            
        print("HTM FILES", self.htm_files)
        
        
    def parse(self, response):
        h = response.css('a.column-documents__link::attr(href)')\
                    .re(r'.*\.htm$')
        page_hrefs = set(h)
        print('LENGTH OF H : ##### ',len(page_hrefs))
        self.htm_files.append(list(page_hrefs))
        #print("\n \n \n \n \n LOOK HERE",h)
        #print("\n \n \n LENGTH", len(h))
        #title = response.css('a[hre]').getall()
        #print("\n \n \n \n \n \nLOOK HERE",title)
        #page = response.url.split("/")[-2]
        #filename = f"hrefs.html"
        #Path(filename).write_bytes(response.body)
        #self.log(f"Saved file {filename}")
        
    def closed(self, reason):
        temp = []
        count = 0 
        for i in self.htm_files:
            for j in i:
                count += 1
                temp.append(j)
        print("HTM FILES", temp[0:10], len(temp), count)
