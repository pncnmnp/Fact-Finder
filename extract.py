import spacy
import textacy.extract
import textacy.ke
import requests
import json
import os.path
import pymongo
from bs4 import BeautifulSoup
from ast import literal_eval
from nltk import sent_tokenize

BASE_URLS_DIR = "./urls/"
BASE_PAGES_DIR = "./pages/"

"""
HOW:
(1) Fetch the top wikipedia links
(2) Request a top link (Mediawiki) - parse it for interesting facts
(2.1) Send a request via MediaWiki API with the link title
(2.2) Parse the keywords from the link body
(2.3) Use each keyword as entity for obtaining semi structured statements - (entity, cue, fragment) triple
- cue – verb lemma with which entity is associated
(2.4) Finding the sentence using its (entity, cue, fragment) triple
(2.5) Using the paper's techniques like - Linguistic Features - Superlative Words, Contradictory Words, Root Word of Sentence, Subject of Sentence, Readability Score (Gunning Fog Index)
(3) Store the link's content with the facts in a mongoDB database
(4) Front-end for retrival of facts (if time permits)
Future Scope: Can be made into an online learning model
"""

def load_spacy_model():
    nlp = spacy.load('en_core_web_md')
    print("Loaded Spacy Model .....")
    return nlp

def parse_and_extract_facts(nlp, text, entity):
    doc = nlp(text)
    statements = textacy.extract.semistructured_statements(doc, entity)
    return statements

def fetch_all_links(link):
    req = requests.get(link)
    soup = BeautifulSoup(req.text, 'html.parser')
    # Customized for pages like - https://en.wikipedia.org/wiki/Wikipedia:2019_Top_50_Report
    tables = soup.find_all("table", class_="wikitable")[0].find_all("td")
    hrefs = list()
    for table in tables:
        if len(table.find_all("a")) != 1:
            continue
        else:
            try:
                url = table.find_all("a")[0]["href"]
                if "File:" not in url:
                    hrefs.append(url)
            except:
                continue
    return hrefs

def store_urls(hrefs, year):
    with open(BASE_URLS_DIR + str(year)+".json", 'w', encoding='utf-8') as f:
        json.dump(hrefs, f, ensure_ascii=False, indent=4)

def media_wiki_call(link):
    HEAD = "https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&explaintext&redirects=1&titles="
    TAIL = "&origin=*"
    req = requests.get(HEAD+link.replace("/wiki/", '')+TAIL)

    page = literal_eval(req.text)["query"]["pages"]
    key = list(page.keys())[0]

    text_body = page[key]["extract"]
    title = page[key]["title"]
    pageid = page[key]["pageid"]

    return (text_body, title, pageid)

def store_pages(text, title, facts, keywords, pageid):
    form = {"text": text, "title": title, "facts": facts, "keywords": keywords, "pageid": pageid}
    with open(BASE_PAGES_DIR + title +".json", 'w', encoding='utf-8') as f:
        json.dump(form, f, ensure_ascii=False, indent=4)

def find_facts(spacy_facts, offset=2000):
    all_facts = list()
    for fact in spacy_facts:
        # check for lists with no facts first
        if fact == list():
            continue
        for sub_fact in fact:
            document = str(sub_fact[2].doc)
            fragment = sub_fact[2].text
            base_index = document.index(fragment)
            left = base_index - offset
            right = base_index + offset
            probable_sents = sent_tokenize(document[left: right])
            try:
                full_fact = [f for f in probable_sents if fragment in f][0]
            except:
                continue
            if full_fact not in all_facts:
                all_facts.append(full_fact)
    return all_facts

def mongo_store(mongo_client, mongo_db, mongo_col, text, title, all_facts, keywords, pageid):
    form = {"text": text, "title": title, "facts": all_facts, "keywords": keywords, "pageid": pageid}    
    x = mongo_col.insert_one(form)

if __name__ == "__main__":
    top_wiki_links = {2019: "https://en.wikipedia.org/wiki/Wikipedia:2019_Top_50_Report",
                        2018: "https://en.wikipedia.org/wiki/Wikipedia:2018_Top_50_Report"}

    mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
    mongo_db = mongo_client["facts_database"]
    mongo_col = mongo_db["facts"]

    for year in top_wiki_links:
        if os.path.isfile(BASE_URLS_DIR + str(year)+".json") == False:
            hrefs = fetch_all_links(top_wiki_links[year])
            store_urls(hrefs, year)
    print("Verified Top Wiki Links Paths .....")

    nlp = load_spacy_model()
    for year in top_wiki_links:
        links = json.load(open(BASE_URLS_DIR + str(year)+".json"))
        for link in links:
            print("For Link: {} .....".format(link))
            text, title, pageid = media_wiki_call(link)
            keywords = list(textacy.ke.yake(nlp(text), normalize="lower", topn=10, ngrams=1))
            spacy_facts = [list(parse_and_extract_facts(nlp, text, keyword[0])) for keyword in keywords]
            all_facts = find_facts(spacy_facts)

            print("Storing: {} .....".format(title))
            # store_pages(text, title, all_facts, keywords, pageid)
            mongo_store(mongo_client, mongo_db, mongo_col, text, title, all_facts, keywords, pageid)
