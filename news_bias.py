'''
news_bias.py

Author: Kaan Altan
Date: 2019-11-08
'''

from bs4 import BeautifulSoup
import requests
from time import sleep
from copy import deepcopy
from tqdm import tqdm
import json
from pathlib import Path
import pandas as pd
import logging
from logging.handlers import RotatingFileHandler

def setup_logger(logger_name, log_file, level = logging.DEBUG):
    """Template for setting up logger

    Parameters
    ==========
    logger_name : str
            Name of logging file
    log_file : str
            Path to write log file
    level : int
            logging.<level> set logging level

    Returns
    =======
    None
    """
    logger = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
    handler = logging.handlers.RotatingFileHandler(str(log_file), maxBytes = 1000000, backupCount = 3)
    handler.setFormatter(formatter)

    logger.setLevel(level)
    logger.addHandler(handler)

def setup_loggers():
    """Loggers setup

    Parameters
    ==========
    None

    Returns
    =======
    logging.Logger
        Configured logger objects for appropriate functions
    """
    setup_logger('main', str(log_path / 'main.log'), level = logging.DEBUG)
    setup_logger('scraper', str(log_path / 'scraper.log'), level = logging.DEBUG)

    main = logging.getLogger('main')
    scraper = logging.getLogger('scraper')

    return main, scraper

'''
- Media Bias table has 3 pages on the website
- Since there are only 3 pages, there's no need to implement a
logic to iterate through pages
- Instead, pages can be fed into the script as a list
'''
pages = [
    'https://www.allsides.com/media-bias/media-bias-ratings',
    'https://www.allsides.com/media-bias/media-bias-ratings?page=1',
    'https://www.allsides.com/media-bias/media-bias-ratings?page=2'
]

def get_agreeance_text(ratio):
    '''Logic to determine agreeance text

    This logic is built in as a Javascript rendered HTML
    It is the 16th .js file in page source
    
    Parameters
    ==========
    ratio : int
        Agreeance ratio from user clicks on the website

    Returns
    =======
    str
        Text corresponding to agreeance ratio
    '''
    if ratio > 3: return "absolutely agrees"
    elif 2 < ratio <= 3: return "strongly agrees"
    elif 1.5 < ratio <= 2: return "agrees"
    elif 1 < ratio <= 1.5: return "somewhat agrees"
    elif ratio == 1: return "neutral"
    elif 0.67 < ratio < 1: return "somewhat disagrees"
    elif 0.5 < ratio <= 0.67: return "disagrees"
    elif 0.33 < ratio <= 0.5: return "strongly disagrees"
    elif ratio <= 0.33: return "absolutely disagrees"
    else: return None

def build_data(pages):
    '''BeatifulSoup scraper

    robots.txt file of allsides.com allows
    one request made every 10 seconds so a
    10 second sleep is built between each
    request
    
    Parameters
    ==========
    pages : list
        List containing path strings to media-bias-ratings pages

    Returns
    =======
    dict
        Dictionary containing scraped data
    '''
    scraper.info("Initializing build_data function (scraper)")
    data= []
    for page in tqdm(pages):
        scraper.info("Considering page: {}".format(page))
        try:
            r = requests.get(page)
            scraper.debug("Request made to page: {}".format(page))
            soup = BeautifulSoup(r.content, 'html.parser')
            scraper.debug("Soup created")
            rows = soup.select('tbody tr')
            scraper.debug("All table rows (<tr>) child of body tag (<tbody>) extracted")
        except:
            scraper.warning("Failed to get page: {}".format(page), exc_info = True)

        for row in tqdm(rows):
            try:
                d = dict()

                d['name'] = row.select_one('.source-title').text.strip()
                d['allsides_page'] = 'https://www.allsides.com' + row.select_one('.source-title a')['href']
                d['bias'] = row.select_one('.views-field-field-bias-image a')['href'].split('/')[-1]
                d['agree'] = int(row.select_one('.agree').text)
                d['disagree'] = int(row.select_one('.disagree').text)
                d['agree_ratio'] = d['agree'] / d['disagree']
                d['agreeance_text'] = get_agreeance_text(d['agree_ratio'])

                scraper.debug("Attempting request to inner page to extract media outlet website: {}".format(d['allsides_page']))
                sleep(10)
                rr = requests.get(d['allsides_page'])
                scraper.debug("Request made to inner page: {}".format(d['allsides_page']))
                ssoup = BeautifulSoup(rr.content, 'html.parser')
                scraper.debug("Inner page soup created")
                d['news_page'] = ssoup.select('.dynamic-grid a')[0]['href']
                scraper.debug("Dictionary complete: {}".format(d))
                data.append(d)
            except:
                scraper.warning("Failed to get inner page: {}".format(d['allsides_page']), exc_info = True)
        sleep(10)
    scraper.info("Scraping complete")
    return data

def save_json(data):
    '''Save scraped data

    Since it takes long to acquire all data, 
    it is a good idea to save it when finished 
    scraping to not lose it in the case an 
    unexpected error happens
    
    Parameters
    ==========
    data : dictionary
        Dictionary containing scraped data for each media outlet

    Returns
    =======
    None
    '''
    try:
        with open(json_path / 'allsides.json', 'w') as f:
            json.dump(data, f)
            main.info("Saved scraped dictionary to json file")
    except:
        main.warning("Error raised trying to save data to json:", exc_info = True)
    

def build_dataframe():
    '''Build pandas dataframe from json data

    Converting our json data to a pandas dataframe will
    allow us to easily edit, manipulate and filter data
    for future use
    
    Parameters
    ==========
    json_data : json object
        Json object containing scraped & saved data for each media outlet

    Returns
    =======
    pandas dataframe
        Pandas dataframe object containing converted from the saved json data
    '''
    try:
        df = pd.read_json(open(json_path / 'allsides.json', 'r'))
        df = df[['name', 'news_page', 'allsides_page', 'bias', 'agree', 'disagree', 'agree_ratio', 'agreeance_text']]
        df.rename(columns = {'name':'Name', 'news_page':'News_Page', 'allsides_page':'Allsides_Page', 'bias':'Media_Bias', 'agree':'Agree_Count', 'disagree':'Disagree_Count',
        'agree_ratio':'Agreeance_Ratio', 'agreeance_text':'Agreeance'}, inplace = True)
        main.info("Dataframe built from json file")
    except:
        main.warning("No data in json file", exc_info = True)
        return pd.DataFrame({})
    return df

def save_csv(dataframe):
    '''Save pandas dataframe created by build_dataframe function to csv

    Saving our built dataframe as a csv will allow
    us to access this data from a multitude of
    applications without coding later on
    
    Parameters
    ==========
    pandas dataframe
        Pandas dataframe object containing scraped data

    Returns
    =======
    None
    '''
    try:
        with open(csv_path / "allsides_bias.csv", 'w', newline = "") as f:
            dataframe.to_csv(f, index = False)
    except:
        main.warning("Error raised trying to save dataframe to csv:", exc_info = True)

if __name__ == '__main__':
    global csv_path, json_path, log_path
    csv_path = Path(r".\CSV")
    json_path = Path(r".\JSON")
    log_path = Path(r".\LOGS")

    main, scraper = setup_loggers()
    
    main.info("Initializing news_bias.py")

    data = build_data(pages)
    main.info("Data dictionary completed")
    save_json(data)
    main.info("Data saved to json file")
    dataframe = build_dataframe()
    main.info("Dataframe built: {}".format(dataframe.to_string()))
    save_csv(dataframe)
    main.info("CSV Saved, check folder: {}".format(csv_path))







