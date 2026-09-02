from django.urls import path

from .metrics import metrics_view
from .views import clusters, namespaces, start_task, test_connection_view, apps, app_detail, app_pods, backups, backup_detail, backup_list


urlpatterns = [
    path("clusters/", clusters),

    path(
        "clusters/<int:cluster_id>/namespaces/",
        namespaces,
    ),

    path(
        "clusters/<int:cluster_id>/namespaces/<int:namespace_id>/",
        namespaces,
    ),
    
    path("task/", start_task),
    path("clusters/test/", test_connection_view),
    path("apps/", apps),
    path("apps/<int:app_id>/", app_detail),
    path("apps/<int:app_id>/pods/", app_pods),
    path("backup/", backups),
    path("backup/details/<str:backup_id>/", backup_detail),
    path("backup/list/", backup_list),
    path("metrics/", metrics_view),
]