import scrapy
from ..items import MeetingItem, AgendaItem, SpeechItem
from bs4 import BeautifulSoup
from scrapy.http import TextResponse
import requests
import pandas as pd


class FolketingetSpider(scrapy.Spider):
    name = "folketinget"

    custom_settings = {
        "FEEDS": {
            "data_meeting.csv": {
                "format": "csv",
                "overwrite": True,
                "fields": ["meeting_id", "url", "date"],
            },
            "data_agenda.csv": {
                "format": "csv",
                "overwrite": True,
                "fields": ["meeting_id", "agenda_item_id", "title", "type"],
            },
            "data_speech.csv": {
                "format": "csv",
                "overwrite": True,
                "fields": [
                    "meeting_id",
                    "agenda_item_id",
                    "speech_item_id",
                    "time_start",
                    "time_end",
                    "speaker_name",
                    "speaker_party",
                    "speaker_role",
                    "speaker_title",
                    "speech_item_text",
                ],
            },
        }
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
                if (
                    current_element.name == "hr"
                    and "Skillestreg" in current_element.get("class", [])
                ):
                    break
                # Append the current element to the section
                agenda_item.append(str(current_element))
                # Move to the next sibling element
                current_element = current_element.find_next_sibling()
            # Combine the elements within the section and append it to the list of sections
            agenda_items.append("".join(agenda_item))
        agenda_items = [
            BeautifulSoup(agenda_item, "html.parser") for agenda_item in agenda_items
        ]
        return agenda_items

    def seperate_agenda_item_speech(self, agenda_item_seperators):
        agenda_item_speeches = []
        for separator_element in agenda_item_seperators:
            agenda_item_speech = []
            current_element = separator_element.find_next_sibling()
            while current_element:
                # Check if the current element is another separator
                if (
                    current_element.name == "meta"
                    and current_element.get("name") == "Start MetaSpeakerMP"
                ):
                    break
                # Append the current element to the section
                agenda_item_speech.append(str(current_element))
                # Move to the next sibling element
                current_element = current_element.find_next_sibling()
            # Combine the elements within the section and append it to the list of sections
            agenda_item_speeches.append("".join(agenda_item_speech))
        agenda_item_speeches = [
            BeautifulSoup(item, "html.parser") for item in agenda_item_speeches
        ]
        return agenda_item_speeches

    def seperate_agenda_item_questions(self, agenda_item_question_seperators):
        agenda_item_questions = []
        for separator_element in agenda_item_question_seperators:
            agenda_item_question = []
            current_element = separator_element.find_next_sibling()
            while current_element:
                # Check if the current element is another separator
                if (
                    current_element.name == "meta"
                    and current_element.get("name") == "SubItemNo"
                ):
                    break
                # Append the current element to the section
                agenda_item_question.append(str(current_element))
                # Move to the next sibling element
                current_element = current_element.find_next_sibling()
            # Combine the elements within the section and append it to the list of sections
            agenda_item_questions.append("".join(agenda_item_question))
        agenda_item_questions = [
            BeautifulSoup(item, "html.parser") for item in agenda_item_questions
        ]
        return agenda_item_questions

    def find_data_agenda(self, agenda_item):
        agenda_title = agenda_item.select_one('meta[name="ShortTitle"]').get("content")
        return agenda_title

    def find_data_speech(self, speech_item):
        time_start = speech_item.select_one('meta[name="StartDateTime"]').get("content")
        try:
            time_end = speech_item.select_one('meta[name="EndDateTime"]').get("content")
        except AttributeError:
            time_end = None
        speaker_first_name = speech_item.select_one('meta[name="OratorFirstName"]').get(
            "content"
        )
        speaker_last_name = speech_item.select_one('meta[name="OratorLastName"]').get(
            "content"
        )
        speaker_name = str(speaker_first_name) + " " + str(speaker_last_name)
        speaker_party = speech_item.select_one('meta[name="GroupNameShort"]').get(
            "content"
        )
        speaker_role = speech_item.select_one('meta[name="OratorRole"]').get("content")

        try:
            speaker_title = speech_item.select_one('p[class="TalerTitel"]').get_text(
                strip=True
            )
        except AttributeError:
            speaker_title = None

        if speaker_title is None:
            try:
                speaker_title = speech_item.select_one(
                    'p[class="TalerTitelMedTaleType"]'
                ).get_text(strip=True)
            except AttributeError:
                speaker_title = None

        speech_item_text = [
            e.get_text()
            for e in speech_item.find_all(
                "p", class_=["Tekst", "TekstLuft", "TekstIndryk"]
            )
        ]
        speech_item_text = " ".join(speech_item_text)
        return (
            time_start,
            time_end,
            speaker_name,
            speaker_party,
            speaker_role,
            speaker_title,
            speech_item_text,
        )

    def write_data_meeting(self, meeting_id, url, date):
        d = MeetingItem()
        d["meeting_id"] = meeting_id
        d["url"] = url
        d["date"] = date
        yield d

    def write_data_agenda(self, meeting_id, agenda_item_id, title, type):
        d = AgendaItem()
        d["meeting_id"] = meeting_id
        d["agenda_item_id"] = agenda_item_id
        d["title"] = title
        d["type"] = type
        yield d

    def write_data_speech(
        self,
        meeting_id,
        agenda_item_id,
        speech_item_id,
        time_start,
        time_end,
        speaker_name,
        speaker_party,
        speaker_role,
        speaker_title,
        speech_item_text,
    ):
        d = SpeechItem()
        d["meeting_id"] = meeting_id
        d["agenda_item_id"] = agenda_item_id
        d["speech_item_id"] = speech_item_id
        d["time_start"] = time_start
        d["time_end"] = time_end
        d["speaker_name"] = speaker_name
        d["speaker_party"] = speaker_party
        d["speaker_role"] = speaker_role
        d["speaker_title"] = speaker_title
        d["speech_item_text"] = speech_item_text
        yield d
        self.speech_item_id += 1
        return self.speech_item_id

    def start_requests(self):
        for i in range(1, 12):
            url = f"https://www.ft.dk/da/dokumenter/dokumentlister/referater?startDate=20041005&endDate=20231001&totalNumberOfRecords=2088&pageSize=200&pageNumber={i}"
            self.urls.append(url)

        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
        page = response.css("a.column-documents__link::attr(href)").re(r".*\.htm$")
        page = set(page)
        self.htm_files.append(list(page))

        if len(self.htm_files) == len(self.urls):
            self.htm_files = [
                str(item) for sublist in self.htm_files for item in sublist
            ]  # flatten into single list
            self.htm_files = [
                "https://www.ft.dk" + htm_file for htm_file in self.htm_files
            ]  # add prefix to htm filenames

            for htm_file in self.htm_files:
                response = requests.get(htm_file)
                response.encoding = "utf-8"
                htm_page_soup = BeautifulSoup(
                    response.text, "html.parser", from_encoding="utf-8"
                )

                # write meeting data
                date = htm_page_soup.find("p", class_="UnderTitel").text
                yield from self.write_data_meeting(self.meeting_id, htm_file, date)

                # parsing the whole page
                yield from self.parse_htm_file(htm_page_soup)
                # yield scrapy.Request(url=htm_file, callback=self.parse_htm_file)

    def parse_htm_file(self, response):
        # Seperate into agenda items using "Skillestreg"
        agenda_items_seperators_list = response.find_all("hr", class_="Skillestreg")
        agenda_items = self.seperate_agenda_items(agenda_items_seperators_list)

        # Going over each agenda item
        agenda_item_id = 1
        for agenda_item in agenda_items:
            try:
                agenda_item_number = agenda_item.select_one('meta[name="ItemNo"]').get(
                    "content"
                )
            except AttributeError:
                agenda_item_number = None

            # check if agenda_item is relevant (e.g. excluding "Meddelser fra formanden" etc.)
            if agenda_item_number != "0" and agenda_item_number is not None:
                agenda_title = agenda_item.select_one('meta[name="ShortTitle"]').get(
                    "content"
                )

                # Check if it is a normal meeting (not "Spørgetid/Spørgetime")
                if (
                    "(spørgetid)" not in agenda_title
                    and "(spørgetime)" not in agenda_title
                ):
                    # seperate into each speech item
                    agenda_item_seperators_list = agenda_item.find_all(
                        "meta", attrs={"name": "Start MetaSpeakerMP"}
                    )
                    agenda_item_speeches = self.seperate_agenda_item_speech(
                        agenda_item_seperators_list
                    )

                    self.speech_item_id = 1
                    for speech_item in agenda_item_speeches:
                        (
                            time_start,
                            time_end,
                            speaker_name,
                            speaker_party,
                            speaker_role,
                            speaker_title,
                            speech_item_text,
                        ) = self.find_data_speech(speech_item)

                        yield from self.write_data_speech(
                            self.meeting_id,
                            agenda_item_id,
                            self.speech_item_id,
                            time_start,
                            time_end,
                            speaker_name,
                            speaker_party,
                            speaker_role,
                            speaker_title,
                            speech_item_text,
                        )

                    type = "0"
                    agenda_title = self.find_data_agenda(agenda_item)
                    yield from self.write_data_agenda(
                        self.meeting_id, agenda_item_id, agenda_title, type
                    )
                    agenda_item_id += 1

                # check if agenda_item is 'spørgetime' or 'spørgetid'
                if any(
                    keyword in agenda_title
                    for keyword in (
                        "(spørgetid)",
                        "Spørgetid",
                        "(spørgetime)",
                        "Spørgetime",
                    )
                ):
                    # seperate into each question
                    agenda_item_question_seperator_list = agenda_item.find_all(
                        "meta", attrs={"name": "SubItemNo"}
                    )
                    agenda_item_questions = self.seperate_agenda_item_questions(
                        agenda_item_question_seperator_list
                    )

                    for question in agenda_item_questions:
                        # seperato into each speech item
                        question_item_seperators_list = question.find_all(
                            "meta", attrs={"name": "Start MetaSpeakerMP"}
                        )
                        question_item_speeches = self.seperate_agenda_item_speech(
                            question_item_seperators_list
                        )

                        self.speech_item_id = 1
                        for speech_item in question_item_speeches:
                            (
                                time_start,
                                time_end,
                                speaker_name,
                                speaker_party,
                                speaker_role,
                                speaker_title,
                                speech_item_text,
                            ) = self.find_data_speech(speech_item)

                            yield from self.write_data_speech(
                                self.meeting_id,
                                agenda_item_id,
                                self.speech_item_id,
                                time_start,
                                time_end,
                                speaker_name,
                                speaker_party,
                                speaker_role,
                                speaker_title,
                                speech_item_text,
                            )

                        type = (
                            "1"
                            if any(
                                keyword in agenda_title
                                for keyword in ("(spørgetid)", "Spørgetid")
                            )
                            else "2"
                        )
                        agenda_title = self.find_data_agenda(question)
                        yield from self.write_data_agenda(
                            self.meeting_id, agenda_item_id, agenda_title, type
                        )
                        agenda_item_id += 1

        self.meeting_id += 1
