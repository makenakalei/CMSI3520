import mechanicalsoup as ms
import redis 
import configparser
from elasticsearch import Elasticsearch, helpers

import pandas as pd
import numpy as np

from neo4j import GraphDatabase

class Neo4JConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)
    
    def add_links(self, page, links):
        with self.driver.session() as session:
            session.execute_write(self._create_links, page, links)

    @staticmethod
    def _create_links(tx, page, links):
        for link in links:
            result = tx.run("CREATE (:Page { url: $link }) -[:LINKS_TO]-> (:Page {url: $page} )", page=page, link=str(link))


neo4j_connector = Neo4JConnector("bolt://localhost:7689", "neo4j", "databases")
#connector.print_greeting("hello y'all")
#neo4j_connector.add_links(page, links)


config = configparser.ConfigParser()
config.read('example.ini')

es = Elasticsearch(
    cloud_id=config['ELASTIC']['cloud_id'],
    http_auth=(config['ELASTIC']['user'], config['ELASTIC']['password'])
)
print(es.info())

def write_to_elastic(es, url, html):
    es.index(index='webpages', document={'url': 'url','html': 'html'})


def crawl(browser, r, es, neo4j_connector, url):

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

    neo4j_connector.add_links(url, links)
# while r.llen("links") > 0:
#     crawl(r.rpop("link"))

browser = ms.StatefulBrowser()
r = redis.Redis()

start_url= "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)

while link := r.rpop('links'): 
    if "Jesus" in str(link):
        break
    crawl(browser, r, es, neo4j_connector, link)


neo4j_connector.close()