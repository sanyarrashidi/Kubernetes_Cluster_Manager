import json
import uuid

from kubernetes import client
from kubernetes.stream import stream
from django_celery_beat.models import CrontabSchedule, PeriodicTask


def _build_api_client(cluster):
    configuration = client.Configuration()

    configuration.host = f"https://{cluster.address}"

    if not cluster.token:
        raise ValueError(
            f"Cluster '{cluster.name}' has no token configured"
        )

    token = cluster.token.strip()

    if token.lower().startswith("bearer "):
        token = token[7:].strip()

    configuration.api_key = {
        "authorization": f"Bearer {token}"
    }

    configuration.api_key_prefix = {}
    configuration.verify_ssl = False

    return client.ApiClient(configuration)


def get_kubernetes_api(cluster):
    return client.CoreV1Api(
        _build_api_client(cluster)
    )


def get_kubernetes_apps_api(cluster):
    return client.AppsV1Api(
        _build_api_client(cluster)
    )


def create_namespace(cluster, namespace_name):
    api = get_kubernetes_api(cluster)

    namespace = client.V1Namespace(
        metadata=client.V1ObjectMeta(
            name=namespace_name
        )
    )

    return api.create_namespace(
        body=namespace
    )


def delete_namespace(cluster, namespace_name):
    api = get_kubernetes_api(cluster)

    return api.delete_namespace(
        name=namespace_name
    )


def test_connection(cluster):
    api = get_kubernetes_api(cluster)

    return api.list_namespace()


def create_app(app):
    api = get_kubernetes_apps_api(
        app.namespace.cluster
    )

    deployment = client.V1Deployment(
        metadata=client.V1ObjectMeta(
            name=app.name
        ),
        spec=client.V1DeploymentSpec(
            replicas=app.replicas,
            selector=client.V1LabelSelector(
                match_labels={
                    "app": app.name
                }
            ),
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(
                    labels={
                        "app": app.name
                    }
                ),
                spec=client.V1PodSpec(
                    containers=[
                        client.V1Container(
                            name=app.name,
                            image=app.image,
                            resources=client.V1ResourceRequirements(
                                requests={
                                    "cpu": app.cpu,
                                    "memory": app.memory,
                                },
                                limits={
                                    "cpu": app.cpu,
                                    "memory": app.memory,
                                },
                            ),
                        )
                    ]
                ),
            ),
        ),
    )

    return api.create_namespaced_deployment(
        namespace=app.namespace.name,
        body=deployment,
    )


def get_app_status(app):
    api = get_kubernetes_api(
        app.namespace.cluster
    )

    pods = api.list_namespaced_pod(
        namespace=app.namespace.name,
        label_selector=f"app={app.name}",
    )

    total = len(pods.items)
    ready = 0

    for pod in pods.items:
        if not pod.status.container_statuses:
            continue

        for container in pod.status.container_statuses:
            if container.ready:
                ready += 1

    return {
        "total": total,
        "ready": ready,
    }


def list_app_pods(app):
    api = get_kubernetes_api(
        app.namespace.cluster
    )

    pods = api.list_namespaced_pod(
        namespace=app.namespace.name,
        label_selector=f"app={app.name}",
    )

    result = []

    for pod in pods.items:
        container_statuses = (
            pod.status.container_statuses or []
        )

        ready_count = sum(
            1
            for container in container_statuses
            if container.ready
        )

        result.append(
            {
                "name": pod.metadata.name,
                "phase": pod.status.phase,
                "ready": ready_count,
                "total": len(container_statuses),
            }
        )

    return result


def update_app(app):
    api = get_kubernetes_apps_api(
        app.namespace.cluster
    )

    deployment = api.read_namespaced_deployment(
        name=app.name,
        namespace=app.namespace.name,
    )

    deployment.spec.replicas = app.replicas

    deployment.spec.strategy = (
        client.V1DeploymentStrategy(
            type="RollingUpdate",
            rolling_update=client.V1RollingUpdateDeployment(
                max_surge=0,
                max_unavailable=1,
            ),
        )
    )

    container = (
        deployment
        .spec
        .template
        .spec
        .containers[0]
    )

    container.image = app.image

    container.resources = (
        client.V1ResourceRequirements(
            requests={
                "cpu": app.cpu,
                "memory": app.memory,
            },
            limits={
                "cpu": app.cpu,
                "memory": app.memory,
            },
        )
    )

    return api.replace_namespaced_deployment(
        name=app.name,
        namespace=app.namespace.name,
        body=deployment,
    )


def delete_app(app):
    api = get_kubernetes_apps_api(
        app.namespace.cluster
    )

    return api.delete_namespaced_deployment(
        name=app.name,
        namespace=app.namespace.name,
    )


def get_app_pod(app):
    api = get_kubernetes_api(
        app.namespace.cluster
    )

    pods = api.list_namespaced_pod(
        namespace=app.namespace.name,
        label_selector=f"app={app.name}",
    )

    for pod in pods.items:
        if pod.status.phase == "Running":
            return pod.metadata.name

    return None


def copy_file_from_pod(
    app,
    pod_name,
    source_path,
    destination_path,
):
    api = get_kubernetes_api(
        app.namespace.cluster
    )

    exec_command = [
        "tar",
        "czf",
        "-",
        source_path,
    ]

    resp = stream(
        api.connect_get_namespaced_pod_exec,
        pod_name,
        app.namespace.name,
        command=exec_command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )

    with open(destination_path, "wb") as f:
        while resp.is_open():
            resp.update(timeout=5)

            if resp.peek_stdout():
                f.write(
                    resp.read_stdout(
                        binary=True
                    )
                )

            if resp.peek_stderr():
                error_output = resp.read_stderr()

                if error_output:
                    raise Exception(
                        f"Error copying from pod: "
                        f"{error_output}"
                    )

    resp.close()


def create_periodic_backup(
    app,
    source_path,
    schedule,
):
    fields = schedule.split()

    if len(fields) != 5:
        raise ValueError(
            "Invalid cron expression"
        )

    (
        minute,
        hour,
        day_of_month,
        month_of_year,
        day_of_week,
    ) = fields

    crontab, _ = CrontabSchedule.objects.get_or_create(
        minute=minute,
        hour=hour,
        day_of_month=day_of_month,
        month_of_year=month_of_year,
        day_of_week=day_of_week,
    )

    PeriodicTask.objects.create(
        crontab=crontab,
        name=(
            f"scheduled-backup-"
            f"{app.id}-"
            f"{uuid.uuid4().hex[:8]}"
        ),
        task="api.tasks.run_scheduled_backup",
        args=json.dumps(
            [
                app.id,
                source_path,
            ]
        ),
    )