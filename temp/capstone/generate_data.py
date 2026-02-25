"""
generate_data.py
Generates a realistic e-commerce sales dataset (~120,000 rows) for the capstone demo.
Includes intentional anomalies (nulls, negatives, duplicates) to exercise the pipeline.
"""

import csv
import random
import os
from datetime import datetime, timedelta

# ── Constants ──────────────────────────────────────────────────────────────────
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "data", "sales_raw.csv")
NUM_RECORDS = 120_000
SEED = 42

REGIONS = ["North", "South", "East", "West", "Central"]

STATES = {
    "North":   ["Minnesota", "Wisconsin", "Michigan", "North Dakota", "Montana"],
    "South":   ["Texas", "Florida", "Georgia", "Alabama", "Louisiana"],
    "East":    ["New York", "Pennsylvania", "New Jersey", "Massachusetts", "Connecticut"],
    "West":    ["California", "Oregon", "Washington", "Nevada", "Arizona"],
    "Central": ["Illinois", "Ohio", "Indiana", "Missouri", "Kansas"],
}

CATEGORIES = {
    "Electronics": {
        "Computers":     [("Laptop Pro 15",  899.99), ("Laptop Air 13",   699.99),
                          ("Desktop Tower",  549.99), ("Chromebook",      349.99)],
        "Phones":        [("Smartphone X",   999.99), ("Smartphone Lite", 499.99),
                          ("Tablet 10\"",    449.99), ("Tablet Mini",     299.99)],
        "Accessories":   [("USB-C Hub",       49.99), ("Wireless Mouse",   29.99),
                          ("Mech Keyboard",   89.99), ("Monitor 27\"",    379.99)],
    },
    "Clothing": {
        "Men's":         [("Slim Jeans",      59.99), ("Oxford Shirt",     39.99),
                          ("Wool Blazer",    129.99), ("Running Shorts",   34.99)],
        "Women's":       [("Summer Dress",    49.99), ("Yoga Pants",       44.99),
                          ("Trench Coat",    149.99), ("Linen Blouse",     39.99)],
        "Kids":          [("Kids T-Shirt",    19.99), ("School Backpack",  34.99),
                          ("Rain Jacket",     44.99), ("Sneakers Jr",      39.99)],
    },
    "Home & Kitchen": {
        "Furniture":     [("Standing Desk",  399.99), ("Office Chair",    249.99),
                          ("Bookshelf",      149.99), ("Coffee Table",    199.99)],
        "Appliances":    [("Air Fryer",       89.99), ("Coffee Maker",     59.99),
                          ("Blender Pro",     79.99), ("Toaster Oven",     64.99)],
        "Cookware":      [("Cast Iron Pan",   49.99), ("Knife Set",        89.99),
                          ("Dutch Oven",      74.99), ("Cutting Board",    24.99)],
    },
    "Books": {
        "Technical":     [("Python Cookbook",  45.00), ("SQL for DE",       39.00),
                          ("Clean Code",       35.00), ("DDIA",             55.00)],
        "Business":      [("Atomic Habits",    18.00), ("Zero to One",      16.00),
                          ("The Lean Startup", 17.00), ("Good to Great",    19.00)],
        "Fiction":       [("Dune",             14.00), ("Project Hail Mary",16.00),
                          ("The Martian",      15.00), ("Recursion",        14.00)],
    },
    "Sports": {
        "Fitness":       [("Dumbbell Set",     89.99), ("Yoga Mat",         29.99),
                          ("Resistance Bands", 19.99), ("Pull-up Bar",      39.99)],
        "Outdoor":       [("Hiking Boots",    119.99), ("Tent 4-Person",   179.99),
                          ("Sleeping Bag",     69.99), ("Trekking Poles",   49.99)],
        "Team Sports":   [("Basketball",       29.99), ("Soccer Ball",      24.99),
                          ("Tennis Racket",    79.99), ("Football",         24.99)],
    },
}

SEGMENTS = ["Consumer", "Corporate", "Home Office"]
SHIP_MODES = ["Standard Class", "Second Class", "First Class", "Same Day"]
STATUSES  = ["Delivered", "Shipped", "Returned", "Cancelled"]
STATUS_WEIGHTS = [0.78, 0.12, 0.06, 0.04]


def random_date(start: datetime, end: datetime) -> datetime:
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def build_product_id(category: str, sub: str, name: str) -> str:
    return f"{category[:3].upper()}-{sub[:3].upper()}-{abs(hash(name)) % 10000:04d}"


def generate_records(n: int) -> list[dict]:
    random.seed(SEED)
    start_dt = datetime(2023, 1, 1)
    end_dt   = datetime(2024, 12, 31)

    # Pre-build flat product list for O(1) sampling
    products = []
    for cat, subs in CATEGORIES.items():
        for sub, items in subs.items():
            for name, price in items:
                products.append({
                    "category": cat,
                    "sub_category": sub,
                    "product_name": name,
                    "base_price": price,
                    "product_id": build_product_id(cat, sub, name),
                })

    records = []
    for i in range(1, n + 1):
        prod   = random.choice(products)
        region = random.choice(REGIONS)
        state  = random.choice(STATES[region])
        order_dt = random_date(start_dt, end_dt)
        ship_lag  = timedelta(days=random.randint(1, 14))

        quantity = random.randint(1, 10)
        discount = round(random.choice([0, 0, 0, 5, 10, 15, 20, 25]) / 100, 2)
        unit_price = round(prod["base_price"] * random.uniform(0.9, 1.1), 2)
        status = random.choices(STATUSES, STATUS_WEIGHTS)[0]

        rec = {
            "order_id":        f"ORD-{i:07d}",
            "customer_id":     f"CUST-{random.randint(1, 20000):05d}",
            "product_id":      prod["product_id"],
            "product_name":    prod["product_name"],
            "category":        prod["category"],
            "sub_category":    prod["sub_category"],
            "quantity":        quantity,
            "unit_price":      unit_price,
            "discount_pct":    discount,
            "region":          region,
            "state":           state,
            "order_date":      order_dt.strftime("%Y-%m-%d"),
            "ship_date":       (order_dt + ship_lag).strftime("%Y-%m-%d"),
            "ship_mode":       random.choice(SHIP_MODES),
            "customer_segment":random.choice(SEGMENTS),
            "status":          status,
        }

        # ── Inject anomalies (≈ 3 % of records) ──────────────────────────────
        anomaly = random.random()
        if anomaly < 0.005:
            rec["unit_price"] = None          # missing price
        elif anomaly < 0.010:
            rec["quantity"] = -1              # negative quantity
        elif anomaly < 0.015:
            rec["order_date"] = "bad-date"    # unparseable date
        elif anomaly < 0.020:
            rec["customer_id"] = None         # missing customer
        elif anomaly < 0.025:
            rec["discount_pct"] = 1.5         # invalid discount > 1
        elif anomaly < 0.030:
            # duplicate: re-use previous order_id
            if records:
                rec["order_id"] = records[-1]["order_id"]

        records.append(rec)
    return records


def write_csv(records: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fieldnames = list(records[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
    print(f"[generate_data] Wrote {len(records):,} records → {path}")


if __name__ == "__main__":
    print(f"[generate_data] Generating {NUM_RECORDS:,} sales records …")
    records = generate_records(NUM_RECORDS)
    write_csv(records, OUTPUT_FILE)
    print("[generate_data] Done.")
