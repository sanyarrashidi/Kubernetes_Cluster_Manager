from django.db import models


class Cluster(models.Model):
    name = models.TextField()
    address = models.TextField()
    token = models.TextField(default="")

    def __str__(self):
        return self.name


class Namespace(models.Model):
    cluster = models.ForeignKey(
        Cluster,
        on_delete=models.CASCADE,
        related_name="namespaces"
    )
    name = models.TextField()

    def __str__(self):
        return self.name
    
    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["cluster", "name"],
                name="unique_namespace_per_cluster",
            )
        ]
        
class App(models.Model):
    name = models.CharField(max_length=255)
    namespace = models.ForeignKey(
        Namespace,
        on_delete=models.CASCADE,
        related_name="apps",
    )
    image = models.CharField(max_length=500)
    replicas = models.PositiveIntegerField(default=1)
    cpu = models.CharField(max_length=50, default="500m")
    memory = models.CharField(max_length=50, default="512Mi")

    def __str__(self):
        return self.name
    
class Backup(models.Model):
    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("running", "Running"),
        ("completed", "Completed"),
        ("failed", "Failed"),
    ]

    backup_id = models.CharField(
        max_length=100,
        unique=True
    )
    app = models.ForeignKey(
        App,
        on_delete=models.CASCADE,
        related_name="backups"
    )
    source_path = models.CharField(max_length=1000)
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default="pending"
    )
    file_path = models.CharField(
        max_length=1000,
        blank=True,
        null=True
    )
    schedule = models.CharField(
        max_length=100,
        blank=True,
        null=True
    )
    error_message = models.TextField(
        blank=True,
        null=True
    )
    created_at = models.DateTimeField(
        auto_now_add=True
    )
    started_at = models.DateTimeField(
        blank=True,
        null=True
    )
    completed_at = models.DateTimeField(
        blank=True,
        null=True
    )

    def __str__(self):
        return self.backup_id