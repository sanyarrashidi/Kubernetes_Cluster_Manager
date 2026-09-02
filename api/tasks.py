import os
import uuid

from datetime import timedelta

from asgiref.sync import async_to_sync
from celery import shared_task
from channels.layers import get_channel_layer
from django.utils import timezone
from django.core.cache import cache

from .models import Backup, App, Cluster, Namespace
from .metrics import clusters_total, namespaces_total, apps_by_status
from utils.kubernetes_service import get_app_pod, copy_file_from_pod, get_app_status


@shared_task
def test_task(message):
    print(f"Worker received: {message}")
    return f"Processed: {message}"

@shared_task(
    bind=True,
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_kwargs={"max_retries": 3}
)
def backup_task(self, backup_id):
    backup = Backup.objects.get(
        backup_id=backup_id
    )

    backup.status = "running"
    backup.started_at = timezone.now()
    backup.save(
        update_fields=[
            "status",
            "started_at"
        ]
    )

    try:
        pod_name = get_app_pod(backup.app)

        if pod_name is None:
            raise Exception(
                "No running pod found for this app"
            )

        date = timezone.now().strftime("%Y-%m-%d")

        backup_directory = os.path.join(
            "/app/backups",
            str(backup.app.id),
            date
        )

        os.makedirs(
            backup_directory,
            exist_ok=True
        )

        backup_file = os.path.join(
            backup_directory,
            f"{backup.backup_id}.tar.gz"
        )

        copy_file_from_pod(
            backup.app,
            pod_name,
            backup.source_path,
            backup_file
        )

        backup.status = "completed"
        backup.file_path = backup_file
        backup.completed_at = timezone.now()
        backup.error_message = None

        backup.save(
            update_fields=[
                "status",
                "file_path",
                "completed_at",
                "error_message"
            ]
        )

    except Exception as error:
        backup.status = "failed"
        backup.error_message = str(error)
        backup.completed_at = timezone.now()

        backup.save(
            update_fields=[
                "status",
                "error_message",
                "completed_at"
            ]
        )

        raise
    
@shared_task
def check_stale_backups():
    cutoff = timezone.now() - timedelta(hours=24)

    stale_backups = Backup.objects.filter(
        status="pending",
        created_at__lt=cutoff
    )

    for backup in stale_backups:
        backup.status = "failed"
        backup.error_message = "Backup timed out after 24 hours in pending state"
        backup.completed_at = timezone.now()

        backup.save(
            update_fields=["status", "error_message", "completed_at"]
        )
        
def get_cached_app_status(app):
    cache_key = f"app-status-{app.id}"
    status = cache.get(cache_key)

    if status is None:
        status = get_app_status(app)
        cache.set(cache_key, status, timeout=5)

    return status


@shared_task
def broadcast_app_statuses():
    channel_layer = get_channel_layer()
    apps_payload = []
    status_counts = {"running": 0, "starting": 0, "stopped": 0}

    for app in App.objects.all():
        try:
            status = get_app_status(app)
        except Exception:
            status = {"total": 0, "ready": 0}

        cache.set(f"app-status-{app.id}", status, timeout=5)

        if status["ready"] == app.replicas and status["ready"] > 0:
            label = "running"
        elif status["total"] == 0 or status["ready"] < status["total"]:
            label = "starting"
        else:
            label = "stopped"

        status_counts[label] += 1

        apps_payload.append({
            "id": app.id,
            "namespace_id": app.namespace_id,
            "ready": status["ready"],
            "total": status["total"],
            "replicas": app.replicas,
        })

    clusters_total.set(Cluster.objects.count())
    namespaces_total.set(Namespace.objects.count())
    for label, count in status_counts.items():
        apps_by_status.labels(status=label).set(count)

    async_to_sync(channel_layer.group_send)(
        "cluster-status",
        {
            "type": "status_update",
            "data": {"apps": apps_payload},
        },
    )

@shared_task
def run_scheduled_backup(app_id, source_path):
    app = App.objects.get(id=app_id)
    backup_id = f"bkp_{uuid.uuid4().hex[:8]}"

    backup = Backup.objects.create(
        backup_id=backup_id,
        app=app,
        source_path=source_path
    )

    backup_task.delay(backup.backup_id)