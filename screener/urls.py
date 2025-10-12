from django.urls import path
from .views import home, api_scan
from .views_genai import api_advise_llm, api_llm_health

urlpatterns = [
    path("", home, name="home"),
    path("api/scan", api_scan, name="api_scan"),
    path("api/advise_llm", api_advise_llm, name="api_advise_llm"),
    path("api/llm_health", api_llm_health),
]