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
        #print "    insert"
        es.index(index=index_name, body=cocaine_mention, doc_type="article")

fake_data = {"items":[{"date":"20120101","body":"hey!"}]}
#insert_results(fake_data, "test")

#print "index"
#print es.index(index="test", doc_type="test_doc", id=1, body=fake_data)
#print "get"
#print es.get(index="test", doc_type="test_doc", id=1)
#print "search full date"
#print es.search(index="test", q = 'date:"20120101"')#body={"query": {"match_all": {}}})
#print "search partial date"
#print es.search(index="test", q = 'date:"2012"')#body={"query": {"match_all": {}}})
##print "cocaine search"
##print es.search(index="cocaine", q = 'the')#body={"query": {"match_all": {}}})
search_term = raw_input("Enter search term to build database: ")
get_background = raw_input("Fetch background data? [y/n] ") in ["y", "Y", "yes", "Yes"]
term_url = "http://chroniclingamerica.loc.gov/search/pages/results/?date1={year}&date2={year}&dateFilterType=yearRange&andtext={term}&format=json&page=".format(term=search_term)
bg_url = "http://chroniclingamerica.loc.gov/search/pages/results/?date1={year}&date2={year}&dateFilterType=yearRange&format=json&page="

#print "request"
#r_data = requests.get(cocaine_url.format(year="1900")).json()
#insert_results(r_data, "r_test")
#print es.search(index="r_test")

def download_firstn_results(n, initial_page=1, base_url=term_url, index_name=search_term):
    n = min(n, 56145)
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


# download_firstn_results(99999999, base_url=cocaine_url, db=cocaine_db) # careful running this, will take forever.

available_years = range(1836,1923)


def func(y, url, index_name):
    print "fetching year {year}".format(year=y)
    download_firstn_results(1000, base_url=url.format(year=y), index_name=index_name)

def term_func(y):
    func(y, term_url, search_term)

def bg_func(y):
    func(y, date_url, "bg")

def multi_fetch(f_type, index_name):
    if f_type == "specific":
        f = cocaine_func
    else:
        f = bg_func
    p = multiprocessing.Pool(5)  # increase if needed, beware diminishing returns
    p.map(f, available_years)

print "cocaine data fetching"
multi_fetch("specific", search_term)
if get_background:
    print "background data fetching"
    multi_fetch("general", "background")

