import json
import uuid

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from kubernetes.client.exceptions import ApiException

from .models import Cluster, Namespace, App, Backup
from .tasks import (
    test_task,
    backup_task,
    get_cached_app_status,
)

from utils.kubernetes_service import (
    create_app,
    create_namespace,
    delete_app,
    delete_namespace,
    get_app_status,
    list_app_pods,
    test_connection,
    update_app,
    create_periodic_backup,
)


def error_response(message, status):
    return JsonResponse(
        {"error": message},
        status=status,
    )


def kubernetes_error(error):
    if error.status == 400:
        return error_response(
            "Invalid request to Kubernetes",
            400,
        )

    if error.status == 401:
        return error_response(
            "Kubernetes authentication failed. Check the cluster token.",
            401,
        )

    if error.status == 403:
        return error_response(
            "Permission denied by Kubernetes",
            403,
        )

    if error.status == 404:
        return error_response(
            "Resource not found in Kubernetes",
            404,
        )

    if error.status == 409:
        return error_response(
            "Resource already exists in Kubernetes",
            409,
        )

    if error.status == 422:
        return error_response(
            "Kubernetes rejected the request",
            422,
        )

    return error_response(
        "Kubernetes error",
        502,
    )


@csrf_exempt
def clusters(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)

            cluster = Cluster.objects.create(
                name=data["name"],
                address=data["address"],
                token=data["token"],
            )

            try:
                test_connection(cluster)

            except ApiException as error:
                cluster.delete()

                if error.status == 401:
                    return error_response(
                        "Kubernetes authentication failed. Check the cluster token.",
                        401,
                    )

                return kubernetes_error(error)

            except Exception as error:
                cluster.delete()

                return error_response(
                    f"Could not connect to cluster: {error}",
                    400,
                )

            return JsonResponse(
                {
                    "id": cluster.id,
                    "name": cluster.name,
                    "address": cluster.address,
                },
                status=201,
            )

        except json.JSONDecodeError:
            return error_response(
                "Invalid JSON",
                400,
            )

        except KeyError as error:
            return error_response(
                f"Missing field: {error.args[0]}",
                400,
            )

        except Exception as error:
            return error_response(
                f"Could not create cluster: {error}",
                500,
            )

    elif request.method == "GET":
        clusters = Cluster.objects.all()

        data = [
            {
                "id": cluster.id,
                "name": cluster.name,
                "address": cluster.address,
            }
            for cluster in clusters
        ]

        return JsonResponse(
            data,
            safe=False,
        )

    return error_response(
        "Method not allowed",
        405,
    )


@csrf_exempt
def namespaces(request, cluster_id, namespace_id=None):
    if request.method == "POST":
        try:
            data = json.loads(request.body)

            try:
                cluster = Cluster.objects.get(id=cluster_id)
            except Cluster.DoesNotExist:
                return error_response(
                    "Cluster not found",
                    404,
                )

            namespace_name = data["name"].strip()

            if not namespace_name:
                return error_response(
                    "Namespace name is required",
                    400,
                )

            if Namespace.objects.filter(
                cluster=cluster,
                name=namespace_name,
            ).exists():
                return error_response(
                    "Namespace already exists in this cluster",
                    409,
                )

            try:
                create_namespace(
                    cluster,
                    namespace_name,
                )

            except ApiException as error:
                return kubernetes_error(error)

            namespace = Namespace.objects.create(
                cluster=cluster,
                name=namespace_name,
            )

            return JsonResponse(
                {
                    "id": namespace.id,
                    "cluster_id": cluster.id,
                    "name": namespace.name,
                },
                status=201,
            )

        except json.JSONDecodeError:
            return error_response(
                "Invalid JSON",
                400,
            )

        except KeyError:
            return error_response(
                "name is required",
                400,
            )

        except Exception as error:
            return error_response(
                f"Could not create namespace: {error}",
                500,
            )

    elif request.method == "GET":
        try:
            cluster = Cluster.objects.get(id=cluster_id)
        except Cluster.DoesNotExist:
            return error_response(
                "Cluster not found",
                404,
            )

        namespaces = Namespace.objects.filter(
            cluster=cluster
        )

        data = [
            {
                "id": namespace.id,
                "name": namespace.name,
            }
            for namespace in namespaces
        ]

        return JsonResponse(
            data,
            safe=False,
        )

    elif request.method == "DELETE":
        if namespace_id is None:
            return error_response(
                "namespace_id is required",
                400,
            )

        try:
            namespace = Namespace.objects.get(
                id=namespace_id
            )
        except Namespace.DoesNotExist:
            return error_response(
                "Namespace not found",
                404,
            )

        try:
            delete_namespace(
                namespace.cluster,
                namespace.name,
            )

            namespace.delete()

            return JsonResponse(
                {
                    "message": "Namespace deleted successfully"
                },
                status=200,
            )

        except ApiException as error:
            return kubernetes_error(error)

        except Exception as error:
            return error_response(
                f"Could not delete namespace: {error}",
                500,
            )

    return error_response(
        "Method not allowed",
        405,
    )


@csrf_exempt
@require_POST
def start_task(request):
    message = request.POST.get("message")

    if not message:
        return error_response(
            "message is required",
            400,
        )

    try:
        task = test_task.delay(message)

        return JsonResponse(
            {
                "message": "Task started",
                "task_id": task.id,
            },
            status=202,
        )

    except Exception as error:
        return error_response(
            f"Could not start task: {error}",
            500,
        )


@csrf_exempt
def test_connection_view(request):
    cluster_id = request.GET.get("cluster_id")

    if not cluster_id:
        return error_response(
            "Missing query param: cluster_id",
            400,
        )

    try:
        cluster = Cluster.objects.get(id=cluster_id)
    except Cluster.DoesNotExist:
        return error_response(
            "Cluster not found",
            404,
        )

    try:
        namespaces = test_connection(cluster)

    except ApiException as error:
        return kubernetes_error(error)

    except Exception as error:
        return error_response(
            f"Could not connect to cluster: {error}",
            502,
        )

    return JsonResponse(
        {
            "namespaces": [
                namespace.metadata.name
                for namespace in namespaces.items
            ]
        }
    )


@csrf_exempt
def apps(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)

            try:
                namespace = Namespace.objects.get(
                    id=data["namespace_id"]
                )
            except Namespace.DoesNotExist:
                return error_response(
                    "Namespace not found",
                    404,
                )

            app = App.objects.create(
                name=data["name"],
                namespace=namespace,
                image=data["image"],
                replicas=data.get("replicas", 1),
                cpu=data.get("cpu", "500m"),
                memory=data.get("memory", "512Mi"),
            )

            try:
                create_app(app)

            except ApiException as error:
                app.delete()
                return kubernetes_error(error)

            return JsonResponse(
                {
                    "id": app.id,
                    "name": app.name,
                    "namespace_id": app.namespace.id,
                    "image": app.image,
                    "replicas": app.replicas,
                    "cpu": app.cpu,
                    "memory": app.memory,
                },
                status=201,
            )

        except json.JSONDecodeError:
            return error_response(
                "Invalid JSON",
                400,
            )

        except KeyError as error:
            return error_response(
                f"Missing field: {error.args[0]}",
                400,
            )

        except Exception as error:
            return error_response(
                f"Could not create app: {error}",
                500,
            )

    elif request.method == "GET":
        try:
            apps = App.objects.all()

            data = []

            for app in apps:
                try:
                    status = get_cached_app_status(app)
                except ApiException:
                    status = {
                        "total": 0,
                        "ready": 0,
                    }

                data.append(
                    {
                        "id": app.id,
                        "name": app.name,
                        "namespace_id": app.namespace.id,
                        "image": app.image,
                        "replicas": app.replicas,
                        "ready": status["ready"],
                        "total": status["total"],
                    }
                )

            return JsonResponse(
                data,
                safe=False,
            )

        except Exception as error:
            return error_response(
                f"Could not load apps: {error}",
                500,
            )

    return error_response(
        "Method not allowed",
        405,
    )


@csrf_exempt
def app_detail(request, app_id):
    try:
        app = App.objects.get(id=app_id)
    except App.DoesNotExist:
        return error_response(
            "App not found",
            404,
        )

    if request.method == "GET":
        try:
            status = get_cached_app_status(app)

        except ApiException as error:
            return kubernetes_error(error)

        return JsonResponse(
            {
                "id": app.id,
                "name": app.name,
                "namespace_id": app.namespace.id,
                "image": app.image,
                "replicas": app.replicas,
                "ready": status["ready"],
                "cpu": app.cpu,
                "memory": app.memory,
                "status": (
                    "running"
                    if status["ready"] == app.replicas
                    and status["ready"] > 0
                    else (
                        "starting"
                        if status["total"] == 0
                        or status["ready"] < status["total"]
                        else "stopped"
                    )
                ),
            }
        )

    elif request.method == "PATCH":
        try:
            data = json.loads(request.body)
        except json.JSONDecodeError:
            return error_response(
                "Invalid JSON",
                400,
            )

        if "name" in data:
            return error_response(
                "App name cannot be changed",
                400,
            )

        allowed_fields = {
            "image",
            "replicas",
            "cpu",
            "memory",
        }

        for field in data:
            if field not in allowed_fields:
                return error_response(
                    f"Invalid field: {field}",
                    400,
                )

        old_values = {
            "image": app.image,
            "replicas": app.replicas,
            "cpu": app.cpu,
            "memory": app.memory,
        }

        if "image" in data:
            app.image = data["image"]

        if "replicas" in data:
            app.replicas = data["replicas"]

        if "cpu" in data:
            app.cpu = data["cpu"]

        if "memory" in data:
            app.memory = data["memory"]

        try:
            update_app(app)

        except ApiException as error:
            app.image = old_values["image"]
            app.replicas = old_values["replicas"]
            app.cpu = old_values["cpu"]
            app.memory = old_values["memory"]

            return kubernetes_error(error)

        app.save()

        return JsonResponse(
            {
                "id": app.id,
                "name": app.name,
                "namespace_id": app.namespace.id,
                "image": app.image,
                "replicas": app.replicas,
                "cpu": app.cpu,
                "memory": app.memory,
            }
        )

    elif request.method == "DELETE":
        try:
            delete_app(app)
            app.delete()

            return JsonResponse(
                {
                    "message": "App deleted successfully"
                },
                status=200,
            )

        except ApiException as error:
            return kubernetes_error(error)

        except Exception as error:
            return error_response(
                f"Could not delete app: {error}",
                500,
            )

    return error_response(
        "Method not allowed",
        405,
    )


@csrf_exempt
def app_pods(request, app_id):
    try:
        app = App.objects.get(id=app_id)
    except App.DoesNotExist:
        return error_response(
            "App not found",
            404,
        )

    if request.method == "GET":
        try:
            pods = list_app_pods(app)

        except ApiException as error:
            return kubernetes_error(error)

        except Exception as error:
            return error_response(
                f"Could not load pods: {error}",
                500,
            )

        return JsonResponse(
            pods,
            safe=False,
        )

    return error_response(
        "Method not allowed",
        405,
    )


@csrf_exempt
def backups(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body)

        except json.JSONDecodeError:
            return error_response(
                "Invalid JSON",
                400,
            )

        if "app_id" not in data:
            return error_response(
                "app_id is required",
                400,
            )

        if "source_path" not in data:
            return error_response(
                "source_path is required",
                400,
            )

        try:
            app = App.objects.get(
                id=data["app_id"]
            )
        except App.DoesNotExist:
            return error_response(
                "App not found",
                404,
            )

        if data.get("schedule"):
            fields = data["schedule"].split()

            if len(fields) != 5:
                return error_response(
                    "Invalid cron schedule",
                    400,
                )

        try:
            backup_id = f"bkp_{uuid.uuid4().hex[:8]}"

            backup = Backup.objects.create(
                backup_id=backup_id,
                app=app,
                source_path=data["source_path"],
                schedule=data.get("schedule"),
            )

            backup_task.delay(
                backup.backup_id
            )

            if data.get("schedule"):
                create_periodic_backup(
                    app,
                    data["source_path"],
                    data["schedule"],
                )

            return JsonResponse(
                {
                    "backup_id": backup.backup_id,
                    "status": backup.status,
                },
                status=202,
            )

        except Exception as error:
            return error_response(
                f"Could not create backup: {error}",
                500,
            )

    return error_response(
        "Method not allowed",
        405,
    )


@csrf_exempt
def backup_detail(request, backup_id):
    if request.method == "GET":
        try:
            backup = Backup.objects.get(
                backup_id=backup_id
            )

        except Backup.DoesNotExist:
            return error_response(
                "Backup not found",
                404,
            )

        return JsonResponse(
            {
                "backup_id": backup.backup_id,
                "app_id": backup.app.id,
                "status": backup.status,
                "error_message": backup.error_message,
            }
        )

    return error_response(
        "Method not allowed",
        405,
    )


@csrf_exempt
def backup_list(request):
    if request.method == "GET":
        app_id = request.GET.get("app_id")

        if not app_id:
            return error_response(
                "app_id is required",
                400,
            )

        backups = Backup.objects.filter(
            app_id=app_id
        )

        data = [
            {
                "backup_id": backup.backup_id,
                "status": backup.status,
            }
            for backup in backups
        ]

        return JsonResponse(
            data,
            safe=False,
        )

    return error_response(
        "Method not allowed",
        405,
    )