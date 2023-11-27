import mechanicalsoup as ms
import redis 
import configparser
from elasticsearch import Elasticsearch, helpers

import pandas as pd
import numpy as np

config = configparser.ConfigParser()
config.read('example.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)
print(es.info())

def write_to_elastic(es, url, html):
    es.index(index='webpages', document={'url': 'url','html': 'html'})


def crawl(browser, r, es, url):

    print("downloading page")
    browser.open(url)

    write_to_elastic(es, url, str(browser.page))

    print("parsing for links")
    a_tags = browser.page.find_all("a")
    hrefs = [ a.get("href") for a in a_tags ]

    wikipedia_domain = "https://en.wikipedia.org"
    print('parsing webpage for links')
    links = [ wikipedia_domain + a for a in hrefs if a and a.startswith("/wiki/") ]
    #print(hrefs)

    r.lpush("links", *links)

# while r.llen("links") > 0:
#     crawl(r.rpop("link"))

browser = ms.StatefulBrowser()
r = redis.Redis()

start_url= "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)

while link := r.rpop('links'): 
    if "Jesus" in str(link):
        break
    crawl(browser, r, es, link)