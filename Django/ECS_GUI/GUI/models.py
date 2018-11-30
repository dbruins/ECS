# Create your models here.
from django.db import models
from django.utils import timezone
import datetime

class pcaModel(models.Model):
    id = models.CharField(max_length=200, primary_key=True)
    permissionTimestamp = models.DateTimeField(null=True)

    def __str__(self):
        return self.id
    class Meta:
        permissions = (
                        ("can_take_control", "can take control"),
                        ("has_control", "has taken control"),
                      )
