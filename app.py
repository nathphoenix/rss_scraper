from flask import Flask
from flask_restful import Api
import rss as rss_scraper
from celery import Celery
import rss_funcs as rss
import extractive_summarizer_funcs as summ
# from article_bart import Summarization
from celery.schedules import crontab
import glob, os


app = Flask(__name__)
app.config["PROPAGATE_EXCEPTIONS"] = True
api = Api(app)

# app.config.from_object("config")
# app.secret_key = app.config['SECRET_KEY']

# # set up celery client
# client = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
# client.conf.update(app.config)



@app.route('/')
def homePage():
	return ("Welcome to the rss api extractor")


# client = Celery('app', broker='pyamqp://guest@localhost//')
client = Celery('app', backend='rpc://', broker='amqp://guest:guest@localhost:5672')

@client.task
def see_you():

    print("See you in ten seconds!")

CELERY_ACCEPT_CONTENT = ['json']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'

# @app.route('/')
@client.task
def get_content():
    rss = rss_scraper()
    return rss


client.conf.beat_schedule = {
    "see-you-in-ten-seconds-task": {
        "task": "app.see_you",
        "task": "app.get_content",
        "schedule": 100.0
    }
}

# client.conf.beat_schedule = {
#     'add-every-30-seconds': {
#         'task': 'app.see_you',
#         'schedule': crontab(minute=1),
#         # 'schedule': 5.0,
#         'args': (16, 16)
#     },
# }
# client.conf.timezone = 'UTC'



app.secret_key = "nathaniel"




if __name__ == "__main__":
    app.run(port=5000, debug=True)
