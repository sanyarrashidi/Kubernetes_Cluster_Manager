import os

from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "hambackend.settings")

app = Celery("hambackend")

app.config_from_object("django.conf:settings", namespace="CELERY")

app.conf.beat_schedule = {
    "check-stale-backups": {
        "task": "api.tasks.check_stale_backups",
        "schedule": 3600.0,
    },
    "broadcast-app-statuses": {
        "task": "api.tasks.broadcast_app_statuses",
        "schedule": 3.0,
    },
}

app.autodiscover_tasks()