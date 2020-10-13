from celery import Celery

app = Celery('periodic', broker="pyamqp://guest@localhost//")


@app.task
def tested():
    print("See you in ten seconds!")


app.conf.beat_schedule = {
    "see-you-in-ten-seconds-task": {
        "task": "periodic.tested",
        "schedule": 10.0
    }
}
