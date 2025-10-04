from django.db import models

class Ticker(models.Model):
    symbol = models.CharField(max_length=16, unique=True)  # e.g., TCS.NS
    name = models.CharField(max_length=128, blank=True)
    in_nifty50 = models.BooleanField(default=False)

    def __str__(self):
        return self.symbol