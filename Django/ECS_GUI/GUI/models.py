# Create your models here.
from django.db import models
from django.utils import timezone
import datetime
from django.conf import settings

class pcaModel(models.Model):
    id = models.CharField(max_length=200, primary_key=True)
    permissionTimestamp = models.DateTimeField(null=True)

    def __str__(self):
        return self.id
    class Meta:
        permissions = (
                        ("can_take_pca_control", "can take pca control"),
                        ("has_pca_control", "has taken control"),
                      )

class ecsModel(models.Model):
    id = models.CharField(max_length=200, primary_key=True)
    permissionTimestamp = models.DateTimeField(null=True)

    def __str__(self):
        return self.id
    class Meta:
        permissions = (
                        ("can_take_ecs_control", "can take ecs control"),
                        ("has_ecs_control", "has taken control"),
                      )

User = settings.AUTH_USER_MODEL

# stores season key associated with an user
class LoggedInUser(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='logged_in_user')
    # Session keys are 32 characters long
    session_key = models.CharField(max_length=32, null=True, blank=True)

    def __str__(self):
        return self.user.username
