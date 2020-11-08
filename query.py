import pymongo
import click
import extract
import textacy.ke

mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["facts_database"]
mongo_col = mongo_db["facts"]

@click.command()
@click.option('--key', default='facts', type=click.Choice(['facts', 'title', 'text', 'keywords', 'pageid']), help='Will query for the particular key in MongoDB')
@click.argument('title')
def query(key, title):
    try:
        results = list(mongo_col.find({"title": {"$regex": title, '$options': 'i'}}))[0][key]
        if key == "facts":
            index = 1
            for result in results:
                print('\033[93m' + "{}::\033[0m\t{}".format(index, result))
                index += 1
    except:
        # print("404: That is all I know!")
        response = input("Not Found. Should I fetch from Wikipedia (y/n): ")
        if response in ["y", "yes"]:
            link = input("Wikipedia Title: ")
            nlp = extract.load_spacy_model()
            text, title, pageid = extract.media_wiki_call(link)
            keywords = list(textacy.ke.yake(nlp(text), normalize="lower", topn=10, ngrams=1))
            spacy_facts = [list(extract.parse_and_extract_facts(nlp, text, keyword[0])) for keyword in keywords]
            all_facts = extract.find_facts(spacy_facts)

            print("Storing: {} .....".format(title))
            extract.store_pages(text, title, all_facts, keywords, pageid)
            extract.mongo_store(mongo_client, mongo_db, mongo_col, text, title, all_facts, keywords, pageid)
            print("Stored: {}. Query Again!".format(title))

if __name__ == '__main__':
    query()