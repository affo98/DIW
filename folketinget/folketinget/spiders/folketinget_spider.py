import scrapy
from ..items import FolketingetItem
from bs4 import BeautifulSoup
from scrapy.http import TextResponse

class FolketingetSpider(scrapy.Spider):
    name = "folketinget"
    
    custom_settings = {
        'FEEDS': { 'data_sample.csv': {
            'format': 'csv',
            'overwrite': True}}
    }
    
    
    def __init__(self, *args, **kwargs):
        super(FolketingetSpider, self).__init__(*args, **kwargs)
        self.htm_files = []
        self.urls = []
        self.meeting_id = 1
        self.speech_item_id = 1


    def seperate_agenda_items(self, agenda_items_seperators):
        agenda_items = []
        for separator_element in agenda_items_seperators:
            agenda_item = []
            current_element = separator_element.find_next_sibling()
            while current_element:
                # Check if the current element is another separator
                if current_element.name == 'hr' and 'Skillestreg' in current_element.get('class', []):
                    break
                # Append the current element to the section
                agenda_item.append(str(current_element))
                # Move to the next sibling element
                current_element = current_element.find_next_sibling()
            # Combine the elements within the section and append it to the list of sections
            agenda_items.append(''.join(agenda_item))
        return agenda_items
    
    
    def seperate_agenda_item_speech(self, agenda_item_seperators):
        agenda_item_speeches = []
        for separator_element in agenda_item_seperators:
            agenda_item_speech = []
            current_element = separator_element.find_next_sibling()
            while current_element:
                # Check if the current element is another separator
                if current_element.name == 'meta' and current_element.get('name') == 'Start MetaSpeakerMP':
                    break
                # Append the current element to the section
                agenda_item_speech.append(str(current_element))
                # Move to the next sibling element
                current_element = current_element.find_next_sibling()
            # Combine the elements within the section and append it to the list of sections
            agenda_item_speeches.append(''.join(agenda_item_speech))
        return agenda_item_speeches
    
    
    def seperate_agenda_item_questions(self, agenda_item_question_seperators):
        agenda_item_questions = []
        for separator_element in agenda_item_question_seperators:
            agenda_item_question = []
            current_element = separator_element.find_next_sibling()
            while current_element:
                # Check if the current element is another separator
                if current_element.name == 'meta' and current_element.get('name') == 'SubItemNo':
                    break
                # Append the current element to the section
                agenda_item_question.append(str(current_element))
                # Move to the next sibling element
                current_element = current_element.find_next_sibling()
            # Combine the elements within the section and append it to the list of sections
            agenda_item_questions.append(''.join(agenda_item_question))
        return agenda_item_questions

    
    def yield_data(self, agenda_item_id, question_item_id, speech_item_id, htm_page_url, speech_item_text):
        d = FolketingetItem()
        d['meeting_id'] = self.meeting_id
        d['agenda_item_id'] = agenda_item_id
        d['question_item_id'] = question_item_id
        d['speech_item_id'] = speech_item_id
        d['url'] = htm_page_url
        d['speech_item_text'] = speech_item_text
        
        self.speech_item_id += 1
        yield d
        return self.speech_item_id
        
    
    def start_requests(self):
        for i in range(1,12):
            url = f"https://www.ft.dk/da/dokumenter/dokumentlister/referater?startDate=20041005&endDate=20231001&totalNumberOfRecords=2088&pageSize=200&pageNumber={i}"
            self.urls.append(url)
            
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse_page)

            
    def parse_page(self, response):
        page = response.css('a.column-documents__link::attr(href)')\
                    .re(r'.*\.htm$')
        page = set(page)
        self.htm_files.append(list(page))
        
        if len(self.htm_files) == len(self.urls):
            self.htm_files = [str(item) for sublist in self.htm_files for item in sublist] #flatten into single list
            self.htm_files = ['https://www.ft.dk' + htm_file for htm_file in self.htm_files] #add prefix to htm filenames

            for htm_file in self.htm_files[0:1]:
                yield scrapy.Request(url=htm_file, callback=self.parse_htm_file)
                
              
    def parse_htm_file(self, response):
        htm_page_url = str(response).split(" ")[1][:-1]
        htm_page_soup = BeautifulSoup(response.text, 'html.parser')
        
        #Seperate into agenda items using "Skillestreg"
        agenda_items_seperators_list = htm_page_soup.find_all('hr', class_='Skillestreg')
        agenda_items = self.seperate_agenda_items(agenda_items_seperators_list)
        
        #Going over each agenda item 
        agenda_item_id = 1
        for agenda_item in agenda_items:
            agenda_item = TextResponse(url = htm_page_url, body = agenda_item, encoding='utf-8')
            agenda_item_number = agenda_item.css('meta[name="ItemNo"]::attr(content)').get()
        
            #check if agenda_item is relevant (e.g. excluding "Meddelser fra formanden" etc.)
            if agenda_item_number != "0" and agenda_item_number is not None:
                agenda_title = agenda_item.css('meta[name="ShortTitle"]::attr(content)').get()
                
                #Check if it is a normal meeting (not "Spørgetid/Spørgetime")
                if "(spørgetid)" not in agenda_title and "(spørgetime)" not in agenda_title: 
                    #seperate into each speech item
                    agenda_item_soup = BeautifulSoup(agenda_item.text, 'html.parser')
                    agenda_item_seperators_list = agenda_item_soup.find_all('meta', attrs={'name': 'Start MetaSpeakerMP'})
                    agenda_item_speeches = self.seperate_agenda_item_speech(agenda_item_seperators_list)
                    
                    self.speech_item_id = 1
                    for speech_item in agenda_item_speeches:
                        speech_item = TextResponse(url = htm_page_url, body = speech_item, encoding='utf-8')
                        speech_item_text = speech_item.css('p.Tekst::text, p.TekstIndryk::text').getall()
                        speech_item_text = ' '.join(speech_item_text)
                        
                        question_item_id = "None"
                        yield from self.yield_data(agenda_item_id, question_item_id, self.speech_item_id, htm_page_url, speech_item_text)
                        

                #check if agenda_item is 'spørgetime' or 'spørgetid'
                if "(spørgetid)" in agenda_title or "(spørgetime)" not in agenda_title:
                    #seperate into each question
                    agenda_item_question_soup = BeautifulSoup(agenda_item.text, 'html.parser')
                    agenda_item_question_seperator_list = agenda_item_question_soup.find_all('meta', attrs={'name': 'SubItemNo'})
                    agenda_item_questions = self.seperate_agenda_item_questions(agenda_item_question_seperator_list)
                    
                    question_item_id = 1
                    for question in agenda_item_questions:
                        #seperato into each speech item
                        question_soup = BeautifulSoup(question, 'html.parser')
                        question_item_seperators_list = question_soup.find_all('meta', attrs={'name': 'Start MetaSpeakerMP'})
                        question_item_speeches = self.seperate_agenda_item_speech(question_item_seperators_list)
                        
                        self.speech_item_id = 1
                        for speech_item in question_item_speeches:
                            speech_item = TextResponse(url = htm_page_url, body = speech_item, encoding='utf-8')
                            speech_item_text = speech_item.css('p.Tekst::text, p.TekstIndryk::text').getall()
                            speech_item_text = ' '.join(speech_item_text)
                            
                            yield from self.yield_data(agenda_item_id, question_item_id, self.speech_item_id, htm_page_url, speech_item_text)
                               
                        question_item_id += 1
                            
                agenda_item_id += 1
                print("+1 AGENDA ITEM ID############", agenda_item_id)
        
        self.meeting_id += 1
        print("+1 MEETING ITEM ID############", self.meeting_id)
            

        # for _ in agenda:
        #     d = FolketingetItem()
        
        #     url = str(response).split(" ")[1][:-1]
        #     title = response.css('p.Titel::text').get()
        #     date = response.css('meta[name="DateOfSitting"]::attr(content)').get().split("T", 1)[0]
        #     time = response.css('meta[name="DateOfSitting"]::attr(content)').get().split("T", 1)[1]
        #     day = response.css('p.UnderTitel::text').get().split(" ", 1)[0]
        #     agenda = response.css('p.DagsordenPunkt span.Bold::text').getall() 
            
        #     d['url'] = url
        #     d['title'] = title
        #     d['date'] = date
        #     d['time'] = time
        #     d['day'] = day
        #     d['agenda'] = agenda
            
        #     yield d
        
       
        
            
    
        
    #def closed(self, reason):
    #      print("HTM FILES##########", self.titles)
    #      print(self.htm_files[0:10])
