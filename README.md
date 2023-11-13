#  Political climate

### Project for Data in the Wild: Wrangling and Visualising Data
###  Datasets
There are four datasets in the folder *folketinget/data/* as outlined in the following table:
| Dataset name | Number of rows | Number of columns | Size (MB)|
| -------- | -------- | -------- | -------- |
| *data_meeting* | 1,787 |5  | 0.066
| *data_agenda* | 15,242 | 9 | 96
| *data_speech* | 369,723 |12  | 103
| *parliament_members* | 1,074 | 4 | 0.013

To create the datasets, run the following scripts:

| Step | Script name | How to run| Description| Output |
| -------- | -------- | -------- | -------- | -------- |
| 1| *folketinget_spider.py* |Run `scrapy crawl folketinget` in shell from folder *scrapy/* |Scrapy spider that crawls parliament meetings. | **Raw** datasets *data_meeting*, *data_agenda*, and *data_speech*. 
| 2|*scraping_postprocess.py* |Run `Python scraping_postprocess.py` in shell| Performs a lot of cleaning, such as date-formatting, handling missing values, removing speech items where moderator is talking, etc. |  **Clean** datasets *data_meeting*, *data_agenda*, and *data_speech*
| 3|*scrape_parliament_members.py* |Run `Python scrape_parliament_members.py` in shell | Scrapes all parliament members and their political party from 2005 to 2023. | Dataset *parliament_members*
| 4| `impute_missing_speaker_party.ipynb` | Run the notebook in Python | Impute missing political party in the *data_speech* using *parliament_members*. | **Non-NA** dataset *data_speech* 

####  Dependencies

We do not have a global set up for dependencies -> manage dependencies locally by pip install or how choose you do it.

  

####  git ignore

Put all files that you do not want to get tracked by git in your \italic{local} .gitignore file

  

####  Branches

Master Branch ------>

  

------> Eisuke Branch

  

------> Andreas Branch . Made Changes to my branch

  

------> Anders Branch - Its created now

  

Anders was here
  
