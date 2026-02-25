"""
simple_pipeline.py
──────────────────────────────────────────────────────────────────────────────
SIMPLE / NAIVE PIPELINE  (comparison baseline – intentionally "bad" design)
──────────────────────────────────────────────────────────────────────────────
Everything is in one flat script:
  • Reads the entire CSV into memory (no generators)
  • Hard-coded file paths (no config files)
  • Zero error handling
  • No logging — only print() calls
  • No abstractions — one giant function
  • No tests possible (logic mixed with I/O)
  • No retry, no parallelism, no async

Run:
    python simple_pipeline.py

Compare its wall-time and memory use against main.py.
"""

import csv
import json
import os
import time

# ── Hard-coded paths  ← no config management ──────────────────────────────────
INPUT_FILE  = os.path.join(os.path.dirname(__file__), "data", "sales_raw.csv")
OUTPUT_DIR  = os.path.join(os.path.dirname(__file__), "output", "simple")
OUTPUT_CSV  = os.path.join(OUTPUT_DIR, "cleaned_sales.csv")
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "aggregated_summary.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── ① Load ENTIRE file into RAM ────────────────────────────────────────────────
print("Reading CSV …")
t0 = time.time()

all_rows = []
with open(INPUT_FILE, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        all_rows.append(dict(row))   # ← full dataset in memory

print(f"  Read {len(all_rows):,} rows in {time.time()-t0:.2f}s")

# ── ② Clean — directly mutate list in place ────────────────────────────────────
print("Cleaning …")
clean_rows = []
seen_ids = set()

for row in all_rows:   # second pass over the same list
    # no try/except — one bad row crashes the whole script
    if not row["order_id"]:
        continue
    if row["order_id"] in seen_ids:
        continue
    seen_ids.add(row["order_id"])

    try:
        qty = int(float(row["quantity"]))
    except (ValueError, TypeError):
        continue
    if qty <= 0:
        continue

    try:
        price = float(row["unit_price"])
    except (ValueError, TypeError):
        continue
    if price <= 0:
        continue

    try:
        disc = float(row["discount_pct"])
    except (ValueError, TypeError):
        continue
    if disc > 1.0:
        continue

    if row["order_date"] == "bad-date":
        continue

    if not row["customer_id"]:
        continue

    # Derived columns
    gross = round(qty * price, 2)
    disc_amt = round(gross * disc, 2)
    net = round(gross - disc_amt, 2)
    cogs = round(net * 0.30, 2)         # fixed 30% — no per-category data
    margin = round(net - cogs, 2)

    row["quantity"]        = qty
    row["unit_price"]      = price
    row["discount_pct"]    = disc
    row["gross_revenue"]   = gross
    row["discount_amount"] = disc_amt
    row["net_revenue"]     = net
    row["cogs"]            = cogs
    row["gross_margin"]    = margin
    clean_rows.append(row)

print(f"  {len(clean_rows):,} rows after cleaning")

# ── ③ Write cleaned CSV — loads all data again ─────────────────────────────────
print("Writing cleaned CSV …")
with open(OUTPUT_CSV, "w", newline="") as f:
    if clean_rows:
        writer = csv.DictWriter(f, fieldnames=list(clean_rows[0].keys()))
        writer.writeheader()
        writer.writerows(clean_rows)   # ← dump entire list at once

# ── ④ Aggregate — another full pass ──────────────────────────────────────────── 
print("Aggregating …")
agg = {}

for row in clean_rows:
    key = (row["region"], row["category"])
    if key not in agg:
        agg[key] = {
            "region": row["region"],
            "category": row["category"],
            "order_count": 0,
            "net_revenue": 0.0,
            "gross_margin": 0.0,
        }
    agg[key]["order_count"] += 1
    agg[key]["net_revenue"]  += row["net_revenue"]
    agg[key]["gross_margin"] += row["gross_margin"]

summary = list(agg.values())
for s in summary:
    s["net_revenue"]  = round(s["net_revenue"], 2)
    s["gross_margin"] = round(s["gross_margin"], 2)

# ── ⑤ Write JSON ───────────────────────────────────────────────────────────────
print("Writing JSON summary …")
with open(OUTPUT_JSON, "w") as f:
    json.dump(summary, f, indent=2)

total_time = time.time() - t0
print(f"\n✅  Simple pipeline done in {total_time:.2f} s")
print(f"   Cleaned rows : {len(clean_rows):,}")
print(f"   Summary rows : {len(summary):,}")
print(f"   Outputs      : {OUTPUT_CSV}")
print(f"                  {OUTPUT_JSON}")
print()
print("=" * 60)
print("  Compare this to main.py (the advanced pipeline):")
print("  → No generators → RAM holds full dataset twice")
print("  → Hard-coded paths → not portable across environments")
print("  → No logging → impossible to diagnose production issues")
print("  → No retry → one network blip kills the run")
print("  → No parallelism → CPU mostly idle during I/O")
print("  → No tests → regressions go undetected")
print("=" * 60)
