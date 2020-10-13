from __future__ import absolute_import
import os
from celery import Celery
import rss_funcs as rss
import extractive_summarizer_funcs as summ


# def make_celery(app_name):
#     celery = Celery(
#         app_name,
#         broker="amqp://localhost//",
#         # broker="amqps://dizvnogv:DCfzIGZ8dIDpSbCjZz7eMkD6_ImjJ7DR@coyote.rmq.cloudamqp.com/dizvnogv"
#     )

#     # celery.Task = ContextTask
#     return celery

# app = Celery('celery', broker="pyamqp://localhost//")


# @app.task
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
                        test = [article_title, published_dates,summary_sentences,categorized_data,max_img,keywords, entity_names]
                        df = pd.DataFrame(test)
                        new = df.T
                        new.rename(columns = {new.columns[0]: "article_title", new.columns[1]: "published_date", new.columns[2]: "summary_sentences", 
                                              new.columns[3]: "categorized_data", new.columns[4]: "max_img",
                                              new.columns[5]: "keywords", new.columns[6]: "entity_names" }, inplace = True )
                        new.to_csv("testing.csv", index=False)
                        ##------------- SAVING TO DATABES -------------
                        import pymongo
                        client = pymongo.MongoClient()
                        client = client['Bloverse']
                        article_collection = client['articles']


                        # db = client.scraper
                        # twitter_user_collection = db.article # similarly if 'testCollection' did not already exist, Mongo would create it
                        df = pd.read_csv('testing.csv')

                        scraped=list(article_collection.find({},{ "_id": 0, "article_tite": 1})) 
                        scraped=list((val for dic in scraped for val in dic.values()))

                        for article_title, published_dates,summary_sentences,categorized_data, max_img, keywords, entity_names in df[['article_title', 'published_date', 'summary_sentences', 'categorized_data', 'max_img', 'keywords', 'entity_names']].itertuples(index=False):
                                if article_title not in scraped:
                                    article_collection.insert_one({'title':article_title, 'publish_date':published_dates, 'Summary': summary_sentences, 'categorized_data': categorized_data,'max_img':max_img,'keywords': keywords, 'entity_names': entity_names}) ####save the df to the collection




        except Exception as e:
            print(e)
            print('We were unable to get the content for this publisher, consider deleting them')

        print('__________________________________________________')


app.conf.beat_schedule = {
    "see-you-in-ten-seconds-task": {
        "task": "celery.get_content",
        "schedule": 10.0
    }
}