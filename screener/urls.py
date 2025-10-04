from django.urls import path
from .views import home, api_scan

urlpatterns = [
    path("", home, name="home"),
    path("api/scan", api_scan, name="api_scan"),
]