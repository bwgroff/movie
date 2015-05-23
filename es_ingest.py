import requests
from datetime import datetime
import multiprocessing
from elasticsearch import Elasticsearch

# In[31]:

#cocaine_db = pymongo.MongoClient().movie.cocaine2 # don't want to overwrite the last batch...
#cocaine_db.create_index("date")
#cocaine_db.create_index("state")
#
#background_db = pymongo.MongoClient().movie.background
#cocaine_db.create_index("date")


# In[ ]:

es = Elasticsearch()

def insert_results(data, db):
    for cocaine_mention in data["items"]:
        _date = cocaine_mention["date"]
        cocaine_mention["date"] = datetime(int(_date[:4]), int(_date[4:6]), int(_date[6:]))
        es.index(index="cocain", cocaine_mention)


# In[ ]:

def download_firstn_results(n, initial_page=1, base_url=cocaine_url, db=cocaine_db):
    n = min(n, 56145)
    this_page = initial_page
    end_index = 0
    while end_index < n:
        try:
            response = requests.get(base_url + str(this_page)).json()
            insert_results(response, db)
        except:
            print "Error processing results on page " + str(this_page) + ". Last end_index is " + str(end_index)+"."
        this_page += 1
        end_index = response["endIndex"]


# In[32]:

cocaine_url = "http://chroniclingamerica.loc.gov/search/pages/results/?andtext=cocaine&format=json&page="
_t = time.time()
# download_firstn_results(99999999, base_url=cocaine_url, db=cocaine_db) # careful running this, will take forever.
print time.time() - _t


# In[ ]:

date_url = "http://chroniclingamerica.loc.gov/search/pages/results/?date1={year}&date2={year}&dateFilterType=yearRange&format=json&page="

available_years = range(1836,1923)

func = lambda y: download_firstn_results(1000, base_url=date_url.format(year=y), db=background_db)
p = multiprocessing.Pool(5)  # increase if needed, beware diminishing returns
p.map(func, available_years)

