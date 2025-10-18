from django.urls import path
from .views import home, api_scan
from .views_genai import api_advise_llm, api_llm_health
from .views_volume import api_volume_spikes
from .views_ma_monthly import api_ma_monthly

urlpatterns = [
    path("", home, name="home"),
    path("api/scan", api_scan, name="api_scan"),
    path("api/advise_llm", api_advise_llm, name="api_advise_llm"),
    path("api/llm_health", api_llm_health),
    path("api/volume_spikes", api_volume_spikes, name="api_volume_spikes"),
    path("api/ma_monthly", api_ma_monthly, name="api_ma_monthly")
]