from django.core.management.base import BaseCommand
from django.conf import settings
from screener.models import Ticker
import csv
from pathlib import Path

class Command(BaseCommand):
    help = "Load NSE universe and mark NIFTY50 from CSVs in data/"

    def handle(self, *args, **options):
        base = Path(settings.BASE_DIR) / "data"
        uni = base / "universe.csv"
        nf = base / "nifty50.csv"

        if not uni.exists() or not nf.exists():
            self.stdout.write(self.style.ERROR("Missing data/universe.csv or data/nifty50.csv"))
            return

        # Read nifty50 list (robust to accidental header, .NS suffix, spaces)
        nifty50 = set()
        with nf.open(newline="") as f:
            for raw in f:
                sym = raw.strip()
                if not sym or sym.upper() in {"SYMBOL", "TICKER"}:
                    continue
                sym = sym.upper().replace(".NS", "")
                nifty50.add(sym)

        created = 0
        updated = 0

        # Read universe with header TICKER,NAME
        with uni.open(newline="") as f:
            reader = csv.DictReader(f)
            if set(reader.fieldnames or []) != {"TICKER", "NAME"}:
                self.stdout.write(self.style.ERROR("universe.csv must have header exactly: TICKER,NAME"))
                return

            for row in reader:
                raw = (row["TICKER"] or "").strip()
                name = (row.get("NAME") or "").strip()
                if not raw:
                    continue

                base_sym = raw.upper().replace(".NS", "")
                sym_yf = f"{base_sym}.NS"  # Yahoo Finance ticker
                in50 = base_sym in nifty50

                obj, is_created = Ticker.objects.update_or_create(
                    symbol=sym_yf,
                    defaults={"name": name, "in_nifty50": in50},
                )
                created += int(is_created)
                updated += int(not is_created)

        nifty_count = Ticker.objects.filter(in_nifty50=True).count()
        total = Ticker.objects.count()
        self.stdout.write(self.style.SUCCESS(
            f"Loaded {created} created, {updated} updated. "
            f"NIFTY50 tagged: {nifty_count} / {total}"
        ))
