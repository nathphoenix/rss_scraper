import feedparser
import pandas as pd
import random
import urllib
import bs4
import os
import shutil
from PIL import Image
import requests
import cv2
import numpy as np
from collections import Counter
headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}


def get_rss_content_df(rss_link):
    """
    Given an rss feed url, this function extracts all the metadata from it and stores in a DF
    """
    feed = feedparser.parse(rss_link)
    source_content_keys = list(feed['entries'][0].keys())

    rss_content_df = pd.DataFrame()
    for keys in source_content_keys:
        temp_keys_list = []
        for article in feed["entries"]:
            try:
                temp_keys_list.append(article[keys])
            except:
                temp_keys_list.append('NA')
        rss_content_df[keys.capitalize()] = temp_keys_list
    
    return rss_content_df

def get_p_tags_from_link(soup):
    """
    This function extracts paragraph tags from the article HTML info
    """
    # get text
    paragraphs = soup.find_all(['p', 'strong', 'em'])

    txt_list = []
    tag_list = []
    
    for p in paragraphs:
        if p.href:
            pass
        else:
            if len(p.get_text()) > 100: # this filters out things that are most likely not part of the core article
                tag_list.append(p.name)
                txt_list.append(p.get_text())

    ## This snippet of code deals with duplicate outputs from the html, helps us clean up the data further
    txt_list2 = []
    for txt in txt_list:
        if txt not in txt_list2:
            txt_list2.append(txt)
    
    return txt_list2


## Build function that takes an RSS link and return the p_tag_reject_list
def get_p_tag_reject_list(rss_link):
    """
    This function takes the 
    """
    rss_content_df = get_rss_content_df(rss_link)
    print(len(rss_content_df))
    
    ## Build functionality to get all the tags across all articles and then detect the ones that need to be weeded out
    article_paragraphs_list = []
    for i in range(min(5,len(rss_content_df))): # We only do it on the first 5 
#         print(i)
        article_title = rss_content_df.iloc[i]['Title']
        article_link = rss_content_df.iloc[i]['Link']

        # Get the p tags across the article
        response = requests.get(article_link, headers=headers)
        soup = bs4.BeautifulSoup(response.text,'lxml')

        # Get the article title
        title = soup.find(['h1','title']).get_text()
        article_text = get_p_tags_from_link(soup)
        article_paragraphs_list += article_text

    ## Now check if any of the sentences have occured more than once
    sentence_count_dict = Counter(article_paragraphs_list)
    paragraph_ignore_dict = Counter({k: sentence_count_dict for k, sentence_count_dict in sentence_count_dict.items() if sentence_count_dict > 1}) # We add

    p_tag_reject_list = (list(paragraph_ignore_dict.keys())) # These are the paragraphs that we're going to pay no attention to and not add to our summarisation pipeline
    
    return p_tag_reject_list