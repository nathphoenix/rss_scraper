"""
RSS Feed Extraction
"""
import feedparser
import pandas as pd
import random
import urllib
from google.cloud import language
from google.oauth2 import service_account
import bs4
import os
import shutil
from PIL import Image
import string
import requests
import operator
import spacy
nlp = spacy.load('en_core_web_md')
# import rake
# import operator
import cv2
import nltk
import numpy as np
from collections import Counter
import dateutil.parser
import datetime
from datetime import date

FILE_NAME = "/phoenix.json"
path = os.getcwd()
new_path = path + FILE_NAME

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= new_path

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

import rss_funcs as rss
import extractive_summarizer_funcs as summ

def get_xml_links():
    celebrity_and_gossip_rss_feeds = [
    
    'https://outoftownblog.com/feed/', 'https://twomonkeystravelgroup.com/feed/',
    'https://www.morninglazziness.com/feed/', 'https://twodrifters.us/feed', 'https://thethrillnation.com/feed/',
    'https://news.xbox.com/en-us/feed/', 'https://www.nintendolife.com/feeds/latest',
    'https://www.gameinformer.com/rss.xml', 'https://www.polygon.com/rss/index.xml', 'https://www.fb101.com/feed/',
    'https://www.autoblog.com/rss.xml', 'https://www.motor1.com/rss/news/all/', 'https://autospies.com/rss.aspx',
    'https://thekoreancarblog.com/feed/',
    'https://tvserieshub.tv/feed/', 'https://www.tellychakkar.com/rss.xml', 'https://www.whatsontv.co.uk/feed/', 
    'https://tvtonight.com.au/feed', 'https://www.spin.com/new-music/feed/',
    'https://www.americanbluesscene.com/feed/','https://www.altpress.com/feed/', 
    'https://pitchfork.com/rss/reviews/best/albums/','https://vulcanpost.com/feed/', 'https://techwireasia.com/feed/',
    'https://www.wired.com/feed/rss', 'https://www.cnet.com/rss/news/', 'https://techcabal.com/feed/',
    'https://www.vox.com/rss/recode/index.xml', 'http://nairametrics.com/feed/',
    'https://www.investmentwatchblog.com/feed/', 'https://www.forbes.com/entrepreneurs/feed2/',
    'https://www.teamtalk.com/feed', 'https://hoopshype.com/feed/','https://www.skysports.com/rss/12040',
    'http://syndication.eonline.com/syndication/feeds/rssfeeds/topstories.xml', 'https://www.tmz.com/rss.xml',
    'https://hollywoodlife.com/feed/', 'https://mtonews.com/.rss/full/', 'https://dontcallitbollywood.com/feed/',
    'https://www.bellanaija.com/feed/', 'https://www.celebitchy.com/feed/']
    celebrity_and_gossip_names = [
    'out of town blogs', 'two monkeys travel',
    'morning lazziness', 'two drifters', 'the thrill nation', 'xbox', 'nintendolife', 'gameinformer', 'polygon', 'fb101',
    'autoblog', 'motor1', 'autospies', 'the korea car blog', 'tvserieshub', 'tellychakkar','What on tv', 'tv_tonight', 
    'Spin', 'american blue scene', 'Altpress', 'Pitch Fork', 'Vulcano Post', 'Techwire Asia','Wired',
    'Cnet', 'tech cabal', 'Vox', 'Nairametrics', 'Investment watch blog', 'Forbes', 'Team talk', 'hoopshype', 'skysport',
    'E Online', 'TMZ', 'Hollywood Life', 'Media Takeout', 'Dont Call It Bollywood', 'Bella Naija', 'Celebitchy'
    ]
    celebrity_and_gossip_rss_dict = dict(zip(celebrity_and_gossip_names, celebrity_and_gossip_rss_feeds))
    return celebrity_and_gossip_rss_dict


def get_extracted_xml():
    rss_name_p_tag_reject_list = []
    celebrity = get_xml_links()
    for name in celebrity.keys():
        print(name)
        rss_link = celebrity[name]
        print(rss_link)
        try:
            p_tag_reject_list = rss.get_p_tag_reject_list(rss_link)
            rss_name_p_tag_reject_list.append(p_tag_reject_list)
        except Exception as e:
            print(e)
            print('An error was raised generating content')

    cat_rss_reject_dict = dict(zip(celebrity.keys(), rss_name_p_tag_reject_list))
    return cat_rss_reject_dict

### Convert this into a function that takes article url as well as reject_p_tags and returns the article text
## This should be run once a day at most
def extract_entity_names(t):
        entity_names = []

        if hasattr(t, 'label') and t.label:
            if t.label() == 'NE':
                entity_names.append(' '.join([child[0] for child in t]))
            else:
                for child in t:
                    entity_names.extend(extract_entity_names(child))

        return entity_names
    
def isPunct(word):
    return len(word) == 1 and word in string.punctuation

def isNumeric(word):
    try:
        float(word) if '.' in word else int(word)
        return True
    except ValueError:
        return False
    

def get_article_metadata(article_url, reject_p_tags, num_sents_in_summary, sentence_threshold):
    """
    This gets content from the article url and also filters out reject p-tags that dont belong
    to the article.
    
    This function gets:
    - Article text
    - Article summarisation
    - Article entities
    - Article categories
    - Article keypoints
    """
    # Get the p tags across the article
    
    response = requests.get(article_url, headers=headers)
    soup = bs4.BeautifulSoup(response.text,'lxml')

    # Get the article title
    title = soup.find(['h1','title']).get_text()
    article_text_sents = rss.get_p_tags_from_link(soup)
    article_texted = [sents for sents in article_text_sents if sents not in reject_p_tags] 
    article_text = ' '.join(article_texted)
#     print('article text', article_text)
    
    ## Summarise article

    summary_sentences = summ.get_article_summary(article_text, num_sents_in_summary, sentence_threshold)
    
    ## Get article entities (Nat)
#     sentences = nltk.sent_tokenize(article_text)
#     tokenized_sentences = [nltk.word_tokenize(sentence) for sentence in sentences]
#     tagged_sentences = [nltk.pos_tag(sentence) for sentence in tokenized_sentences]
#     chunked_sentences = nltk.ne_chunk_sents(tagged_sentences, binary=True)
#     entity_names = []
#     for tree in chunked_sentences:
#         # Print results per sentence
#         # print extract_entity_names(tree)

#         entity_names.extend(extract_entity_names(tree))
    
    
    # new_string1 = str(article_text)

    # doc = nlp(new_string1)

    # # Extract entities
    # doc_entities = doc.ents
    # #     print(doc_entities)
    # # Subset to person type entities
    # doc_persons = filter(lambda x: x.label_ == 'PERSON', doc_entities)
    # doc_persons = filter(lambda x: len(x.text.strip().split()) >= 1, doc_persons)
    # doc_persons = list(map(lambda x: x.text.strip(), doc_persons))
    # #     print(doc_persons)
    # # Assuming that the first Person entity with more than two tokens is the candidate's name
    # country_name = doc_persons
    # #      country_name = doc_country
    # #     print(doc_country)
    # df = pd.DataFrame(list(country_name),columns=['PERSON'])
    # new = df.drop_duplicates( subset = ["PERSON"], keep="first")
    # entity_names = new["PERSON"].values.tolist()

    credentials = service_account.Credentials.from_service_account_file('phoenix.json')
    client = language.LanguageServiceClient(
        credentials=credentials,
     )
    # Create a list of entity types, and then 'app_ent_list' contains the entity types that we store
    enums = ['UNKNOWN','PERSON','LOCATION','ORGANIZATION','EVENT','WORK_OF_ART','CONSUMER_GOOD',
             'OTHER','PHONE_NUMBER','ADDRESS','DATE','NUMBER','PRICE' ]
    app_ent_list = [ 'PERSON','ORGANIZATION','LOCATION','CONSUMER_GOOD','WORK_OF_ART' ]

    document = language.types.Document(
             content=article_text,
             language='en',
             type='PLAIN_TEXT',
    )

    response = client.analyze_entities(
             document=document,
             encoding_type='UTF8',
    )

    entity_names = []
    for entity in response.entities:
        try:
            if enums[entity.type] in app_ent_list:
                entity_names.append(entity.name)
        except IndexError:
            continue

    print(entity_names)

    ## Get article categories (top 3) (Nat)
    line = article_text


    language_client = language.LanguageServiceClient()

    document = language.types.Document(
        content=line,
        type=language.enums.Document.Type.PLAIN_TEXT)
    response = language_client.classify_text(document)
    categories = response.categories

    result = {}

    for category in categories:
            # Turn the categories into a dictionary of the form:
            # {category.name: category.confidence}, so that they can
            # be treated as a sparse vector.
        result[category.name] = category.confidence
    data = result
    df = pd.DataFrame(result.items(), columns=['category','confidence'])
    categorized = df["category"][0]
    new = pd.read_csv("google_category.csv")
    final = new[new["Google_name"] == categorized]
    final_data = final["Bloverse_name"].to_string(index=False)
    categorized_data = final_data.replace(" ", "")


    #IMAGE EXTRACTION
    FOLDER_NAME = "blog_details"
    try:
        if not os.path.exists(FOLDER_NAME):
                os.makedirs(FOLDER_NAME)
    except:
        pass
    images = soup.find('body').find_all('img')

# --- loop ---

    data = []
    i = 0

    for img in images:
    #     print('HTML:', img)

        url = img.get('src')

        if url:  # skip `url` with `None`
        #         print('Downloading:', url)
            try:
                response = requests.get(url, stream=True)

                i += 1
                url = url.rsplit('?', 1)[0]  # remove ?opt=20 after filename
                ext = url.rsplit('.', 1)[-1] # .png, .jpg, .jpeg
                filename = f'{i}.{ext}'
        #             print('Filename:', filename)

                with open(os.path.join(FOLDER_NAME, filename), 'wb') as out_file:
                        shutil.copyfileobj(response.raw, out_file)

                image = cv2.imread(os.path.join(FOLDER_NAME, filename))
#                 with open(filename, 'wb') as out_file:
#                     shutil.copyfileobj(response.raw, out_file)

                height, width = image.shape[:2]

                data.append({
                    'url': url,
                    'path': filename,
                    'width': width,
                    'height': height,
                })

            except Exception as ex:
                pass
    # --- after loop ---
    # print('max:', max(data, key=lambda x:x['width']))
    max_img = sorted(data, key=lambda x:x['width'], reverse=True)[:5]
    max_img = [d.get('url', None) for d in max_img]
#     print(dico)
#     print(max_img)


    #KEYWORDS EXTRACTION
#     from rake_nltk import Rake

#     rake = Rake() # Uses stopwords for english from NLTK, and all puntuation characters.
#     extract = rake.extract_keywords_from_text(article_text)
#     keys = rake.get_ranked_phrases() # To get keyword phrases ranked highest to lowest.
#     new_list = keys[0:25]
#     keywords = [sentence for sentence in new_list if len(sentence.split()) <= 3]
#     print('KEYWORDS', keywords)

    sentences=nltk.sent_tokenize(article_text)
    stopwords=set(nltk.corpus.stopwords.words())

    phrase_list=[]
    for sentence in sentences:

        words=map(lambda x: "|" if x in stopwords else x, nltk.word_tokenize(sentence.lower()))
        phrase=[]
        for word in words:
            if word == '|' or isPunct(word):
                if len(phrase)>0:
                    phrase_list.append(phrase)
                    phrase=[]
            else:
                phrase.append(word)

    word_freq=nltk.FreqDist()
    word_degree=nltk.FreqDist()

    for phrase in phrase_list:
        l=[lambda x: not isNumeric(x), phrase]
        degree=len(l)-1
        for word in phrase:
            word_freq[word]+=1
            word_degree[word]+=degree
    for word in word_freq.keys():
        word_degree[word] = word_degree[word] + word_freq[word] 
    word_scores = {}
    for word in word_freq.keys():
        word_scores[word] = word_degree[word] / word_freq[word]

    phrase_scores = {}
    for phrase in phrase_list:
        phrase_score = 0
        for word in phrase:
            phrase_score += word_scores[word]
        phrase_scores[" ".join(phrase)] = phrase_score    
    sorted_phrase_scores = sorted(phrase_scores.items(), key=operator.itemgetter(1), reverse=True)
    n_phrases = len(sorted_phrase_scores)
    keywords= sorted_phrase_scores[0:int(n_phrases/1)]

    keyed = [x[0] for x in keywords]
    new_list = keyed[0:20]
    keywords = [sentence for sentence in new_list if len(sentence.split()) <= 3]


    return article_text, summary_sentences, entity_names, categorized_data, max_img, keywords

def get_content():
    celebrity_links = get_xml_links()
    cat_rss_reject_dict = get_extracted_xml()
    ## Loop through all the categories
# Initial params
    today = date.today()
    now = datetime.datetime.now()
    curr_hour = now.hour

    # Parameters for article summarisation
    num_sents_in_summary = 3
    sentence_threshold = 100

    ## Loop through all the names in the category dictioanry
    for name in celebrity_links.keys():
        print('__________________________________________________')
        print('Publisher name: %s' % name)
        try:
            rss_link = celebrity_links[name]
            rss_content_df = rss.get_rss_content_df(rss_link)
            print(rss_content_df.columns)
            ## **Add code here to filter out content not published in the last hour
            reject_p_tags = cat_rss_reject_dict[name]

            ## Add code to loop through all the articles in the rss_content_df

            for i in range(len(rss_content_df)):
                article_title = rss_content_df.iloc[i]['Title']
                article_url = rss_content_df.iloc[i]['Link']
                try:
                    published_dates = rss_content_df.iloc[i]['Published']
                except:
                    published_dates = rss_content_df.iloc[i]['Updated']
                published_date_raw = dateutil.parser.parse(published_dates)

                published_date = published_date_raw.date()
                if published_date == today:
                    hour = (published_date_raw.hour)
                    if hour >= curr_hour-3:

                        article_text, summary_sentences, entity_names, categorized_data, max_img, keywords = get_article_metadata(article_url, reject_p_tags, num_sents_in_summary, sentence_threshold)

    #                     data = []
                        test = [article_title, name, published_dates,summary_sentences,categorized_data,max_img,keywords, entity_names]
                        df = pd.DataFrame(test)
                        new = df.T
                        new.rename(columns = {new.columns[0]: "article_title", new.columns[1]: "publisher_name", new.columns[2]: "published_date", new.columns[3]: "summary_sentences", 
                                              new.columns[4]: "categorized_data", new.columns[5]: "max_img",
                                              new.columns[6]: "keywords", new.columns[7]: "entity_names" }, inplace = True )
                        new.to_csv("testing.csv", index=False)
                        ##------------- SAVING TO DATABES -------------
                        import pymongo
                        # for localhost
                        client = pymongo.MongoClient()
                        client = client['Bloverse']
                        article_collection = client['articles']
                    



                        # db = client.scraper
                        # twitter_user_collection = db.article # similarly if 'testCollection' did not already exist, Mongo would create it
                        df = pd.read_csv('testing.csv')

                        scraped=list(article_collection.find({},{ "_id": 0, "article_title": 1})) 
                        scraped=list((val for dic in scraped for val in dic.values()))

                        for article_title, publisher_name, published_dates,summary_sentences,categorized_data, max_img, keywords, entity_names in df[['article_title', 'publisher_name', 'published_date', 'summary_sentences', 'categorized_data', 'max_img', 'keywords', 'entity_names']].itertuples(index=False):
                                if article_title not in scraped:
                                    resulted = article_collection.insert_one({'article_title':article_title, 'publisher_name': publisher_name, 'publish_date':published_dates, 'Summary': summary_sentences, 'categorized_data': categorized_data,'max_img':max_img,'keywords': keywords, 'entity_names': entity_names}) 

                                #----------saving entity only -------------#

                                    if resulted:
                                            
                                        all_data = article_collection.find({})
#                                         x = article_collection.find({},{'_id': 1, 'article_title': 1,  
#                                                          'entity_names': 1 })
                                        key_items = ['article_title', 'publisher_name', 'publish_date', 'Summary', 'categorized_data', 'max_img', 'keywords']
                                        for data in all_data:
                                            for key in list(data.keys()):  # Use a list instead of a view
                                                if key in key_items:
                                                    del data[key]
                                                if key == '_id':
                                                    data['Article_mention'] = data.pop('_id')
                                                
                                            
                                            df_new = pd.DataFrame([data], columns=data.keys())
                                        tested = pymongo.MongoClient()
                                        cliented = tested['Bloverse']
                                        article_collection_ent = cliented['general_ent']


                                        scraper=list(article_collection_ent.find({},{ "_id": 0, "entity_names": 1})) 
                                        scraper=list((val for dic in scraper for val in dic.values()))

                                        for entity_names, Article_mention in df_new[[ 'entity_names', 'Article_mention']].itertuples(index=False):
                                            if entity_names not in scraper:
                                                article_collection_ent.insert_one({'entity_names': entity_names, 'Article_mention':Article_mention}) ####save the df to the collection





        except Exception as e:
            print(e)
            print('We were unable to get the content for this publisher, consider deleting them')

        print('__________________________________________________')

if __name__ == "__main__":
    get_content()
    print('Extraction successfully completed')
    