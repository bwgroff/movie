import time
import requests
from datetime import datetime
import multiprocessing
from elasticsearch import Elasticsearch
import logging

log_filename = "etl.log"

es = Elasticsearch()

def insert_results(data, index_name):
    for cocaine_mention in data["items"]:
        _date = cocaine_mention["date"]
        cocaine_mention["date"] = datetime(int(_date[:4]), int(_date[4:6]), int(_date[6:]))
        es.index(index=index_name, body=cocaine_mention, doc_type="article")


term_url = "http://chroniclingamerica.loc.gov/search/pages/results/?date1={year}&date2={year}&dateFilterType=yearRange&andtext={term}&format=json&page="
bg_url = "http://chroniclingamerica.loc.gov/search/pages/results/?date1={year}&date2={year}&dateFilterType=yearRange&format=json&page="


def download_firstn_results(max_n, initial_page=1, base_url=term_url, index_name=search_term):
    n = min(max_n, 56145)
    this_page = initial_page
    end_index = 0
    while end_index < n:
        try:
            response = requests.get(base_url + str(this_page)).json()
            insert_results(response, index_name)
        except:
            logging.debug("Error in page " + str(this_page))
        this_page += 1
        end_index = response["endIndex"]
        n = min(response["totalItems"], max_n)

available_years = range(1885,1920)


def func(y, url, index_name, count = 56145):
    print "fetching data from {year} for index {index_name}.".format(year=y, index_name=index_name)
    download_firstn_results(count, base_url=url, index_name=index_name)

def term_func(y):
    func(y, term_url.format(year=y, term=search_term), search_term)

def bg_func(y):
    func(y, bg_url.format(year=y), "background")

def multi_fetch(f_type, index_name):
    if f_type == "specific":
        f = term_func
    else:
        f = bg_func
    p = multiprocessing.Pool(5)  # increase if needed, beware diminishing returns
    p.map(f, available_years)

#t0 = time.time()


get_term = raw_input("Fetch new term data? [y/n] ") in ["y", "Y", "yes", "Yes"]
if get_term: 
    search_term = raw_input("Enter search term to build database: ")
get_background = raw_input("Fetch background data? [y/n] ") in ["y", "Y", "yes", "Yes"]
if get_term:
    print "term data fetching"
    multi_fetch("specific", search_term)
#    t1 = time.time()
#    print "Time elapsed while building term-specific database: {time}".format(time=t1-t0)
if get_background:
    base_count = 1000
    print "background data fetching"
    multi_fetch("general", "background")
#    t2 = time.time()
#    print "Time elapsed while building background database: {time}".format(time=t2-t1)
