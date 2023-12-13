#  Climate Trends in the Political Discourse of the Danish Parliament

### Description
This repository contains code and data for the project for Data in the Wild: Wrangling and Visualising Data, Autumn 2024. 

Our project uses publicly available transcriptions from the Danish Parliament to examine how much politicians actually engage in discussions about green policies. 


###  Main Datasets
There are four  main datasets in the folder *folketinget/data/* as outlined in the following table:
| Dataset name | Number of rows | Number of columns | Size (MB)|
| -------- | -------- | -------- | -------- |
| *data_meeting* | 1,787 |5  | 0.054
| *data_agenda* | 15,242 | 4 | 0.6
| *data_speech* | 369,723 |13  | 243
| *parliament_members* | 1,074 | 4 | 0.013

To create the datasets, run the following scripts:

| Step | Script name | How to run| Description| Output |
| -------- | -------- | -------- | -------- | -------- |
| 1| *folketinget_spider.py* |Run `scrapy crawl folketinget` in shell from folder *scrapy/* |Scrapy spider that crawls parliament meetings. | **Raw** datasets *data_meeting*, *data_agenda*, and *data_speech*. 
| 2|*scraping_postprocess.py* |Run `Python scraping_postprocess.py` in shell| Performs a lot of cleaning, such as date-formatting, handling missing values, removing speech items where moderator is talking, etc. |  **Clean** datasets *data_meeting*, *data_agenda*, and *data_speech*
| 3|*scrape_parliament_members.py* |Run `Python scrape_parliament_members.py` in shell | Scrapes all parliament members and their political party from 2005 to 2023. | Dataset *parliament_members*
| 4| *impute_missing_speaker_party.ipynb* | Run the notebook in Python | Impute missing political party in the *data_speech* using *parliament_members*. | **Non-NA** dataset *data_speech* 

### Additional Datasets
In addition to the main datasets, the folder *folketinget/data/* also contains additional datasets:

| Dataset name | Number of rows | Number of columns | Size (MB)| Description | Script |
| -------- | -------- | -------- | -------- | -------- | -------- |
| *data_annotation* | 1,542 |9  | 23 | Sampled ~10\% agenda items while stratifying for year and types of agenda item. | *annotation.ipynb*
| *data_speech_tok* | 369,723 |4  | 130 | Preprocessed speech item texts: Tokenized, lowercased, and stopword removal from Spacy. | *tokenize_speech_data.py*



In addition, the folder *folketinget/data/word_lists/* contains datasets for all the lists of words that are created in the scripts *list_of_words.ipynb* and *augmented_list_of_words.ipynb*. 




  
