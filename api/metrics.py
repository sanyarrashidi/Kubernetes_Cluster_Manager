import os
import random
import time

from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from django.http import HttpResponse


clusters_total = Gauge(
    "kubefleet_clusters_total",
    "Number of registered clusters",
    ["resource"],
)

namespaces_total = Gauge(
    "kubefleet_namespaces_total",
    "Number of namespaces across all clusters",
    ["resource"],
)

apps_by_status = Gauge(
    "kubefleet_apps_by_status",
    "Number of apps in each status",
    ["status"],
)

kubernetes_operations_total = Counter(
    "hamamooz_kubernetes_operations_total",
    "Total number of Kubernetes operations by outcome",
    ["operation", "outcome"],
)

kubernetes_operation_duration_seconds = Histogram(
    "hamamooz_kubernetes_operation_duration_seconds",
    "Duration of Kubernetes operations in seconds",
    ["operation"],
)

backup_jobs_total = Counter(
    "hamamooz_backup_jobs_total",
    "Total number of backup jobs by terminal outcome",
    ["outcome"],
)

backup_duration_seconds = Histogram(
    "hamamooz_backup_duration_seconds",
    "Duration of backup jobs in seconds",
)

backups_in_progress = Gauge(
    "hamamooz_backups_in_progress",
    "Number of backup jobs currently in progress",
)


def metrics_view(request):
    if os.getenv("MOCK_METRICS", "false").lower() == "true":
        update_mock_metrics()

    return HttpResponse(
        generate_latest(),
        content_type=CONTENT_TYPE_LATEST,
    )
    
def update_mock_metrics():
    clusters_total.set(random.randint(1, 10))
    namespaces_total.set(random.randint(5, 50))

    apps_by_status.labels(status="running").set(random.randint(1, 20))
    apps_by_status.labels(status="starting").set(random.randint(0, 5))
    apps_by_status.labels(status="stopped").set(random.randint(0, 10))

    for operation in ["create", "delete", "update"]:
        for outcome in ["success", "failure"]:
            kubernetes_operations_total.labels(
                operation=operation,
                outcome=outcome,
            ).inc(random.randint(0, 3))

    for operation in ["create", "delete", "update"]:
        kubernetes_operation_duration_seconds.labels(
            operation=operation
        ).observe(random.uniform(0.1, 3.0))

    for outcome in ["completed", "failed"]:
        backup_jobs_total.labels(
            outcome=outcome
        ).inc(random.randint(0, 2))

    backup_duration_seconds.observe(
        random.uniform(1.0, 10.0)
    )

    backups_in_progress.set(
        random.randint(0, 5)
    )