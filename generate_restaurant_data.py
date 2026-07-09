"""
Restaurant ordering pipeline — synthetic source data generator
===============================================================

Produces five CSVs shaped like raw extracts from an operational POS system,
ready to land in Snowflake (e.g. via stage + COPY INTO) as your `raw` schema:

    restaurants.csv   (dim)   small chain of locations
    menu_items.csv    (dim)   the menu, with a base price
    customers.csv     (dim)   loyalty customers (orders can also be guests)
    orders.csv        (fact)  one row per order — header only, NO total
    order_items.csv   (fact)  one row per line item, with transaction-time price

Two deliberate design decisions (both there to give your dbt layer real work):

1. Orders carry no `order_total`. You aggregate line items up in dbt. In a real
   POS you'd often store a total for reconciliation — flip GENERATE_ORDER_TOTAL
   to True if you'd rather model the reconciliation case instead.

2. order_items.unit_price is snapshotted at order time, and there's a mid-year
   menu price increase (see PRICE_INCREASE_*). So you CANNOT join order_items
   back to menu_items.base_price to get revenue — you must use the snapshotted
   price. That's the transaction-time-price lesson, made concrete.

Set INTRODUCE_MESSINESS = False for a clean dataset. When True it injects a
small, *enumerated* amount of realistic dirt (see the messiness section) so you
have something for staging cleanup and dbt tests to bite on.

    pip install faker pandas numpy
"""

import os
import random
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------
SEED = 42
N_CUSTOMERS = 800
N_ORDERS = 5_000
START_DATE = date(2024, 1, 1)
END_DATE = date(2024, 12, 31)
OUTPUT_DIR = "raw_data"

GUEST_ORDER_RATE = 0.18          # share of orders with no customer (walk-ins)
GENERATE_ORDER_TOTAL = False     # see design note #1 above
INTRODUCE_MESSINESS = True       # see design note #3 / messiness section below

PRICE_INCREASE_DATE = date(2024, 7, 1)
PRICE_INCREASE_FACTOR = 1.08     # 8% menu bump mid-year

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)
np.random.seed(SEED)

# ----------------------------------------------------------------------------
# STATIC DIMENSIONS
# ----------------------------------------------------------------------------
RESTAURANTS = [
    {"restaurant_id": 1, "name": "The Copper Skillet", "city": "Arlington",   "state": "VA", "opened_date": "2019-03-15"},
    {"restaurant_id": 2, "name": "The Copper Skillet", "city": "Alexandria",  "state": "VA", "opened_date": "2020-09-01"},
    {"restaurant_id": 3, "name": "The Copper Skillet", "city": "Bethesda",    "state": "MD", "opened_date": "2021-06-20"},
    {"restaurant_id": 4, "name": "The Copper Skillet", "city": "Washington",  "state": "DC", "opened_date": "2022-11-10"},
]
# Location 4 is busiest, location 3 newest/quietest
RESTAURANT_WEIGHTS = np.array([0.28, 0.24, 0.18, 0.30])

MENU = [
    # (name, category, base_price)
    ("Loaded Nachos",          "Appetizer", 11.50),
    ("Crispy Calamari",        "Appetizer", 13.00),
    ("Spinach Artichoke Dip",  "Appetizer", 10.50),
    ("Buffalo Wings",          "Appetizer", 12.00),
    ("Soup of the Day",        "Appetizer",  7.00),
    ("House Salad",            "Appetizer",  8.50),
    ("Caesar Salad",           "Appetizer",  9.50),
    ("Classic Cheeseburger",   "Entree",    15.00),
    ("Bacon BBQ Burger",       "Entree",    17.00),
    ("Grilled Salmon",         "Entree",    24.00),
    ("Ribeye Steak",           "Entree",    32.00),
    ("Chicken Parmesan",       "Entree",    21.00),
    ("Fish & Chips",           "Entree",    18.00),
    ("Margherita Pizza",       "Entree",    16.00),
    ("Pepperoni Pizza",        "Entree",    17.00),
    ("Veggie Stir Fry",        "Entree",    15.00),
    ("Shrimp Tacos",           "Entree",    19.00),
    ("Pulled Pork Sandwich",   "Entree",    16.00),
    ("Mac & Cheese",           "Entree",    13.00),
    ("French Fries",           "Side",       5.00),
    ("Sweet Potato Fries",     "Side",       6.00),
    ("Side Salad",             "Side",       5.00),
    ("Onion Rings",            "Side",       6.00),
    ("Garlic Bread",           "Side",       5.00),
    ("Steamed Veggies",        "Side",       5.00),
    ("Cheesecake",             "Dessert",    9.00),
    ("Chocolate Lava Cake",    "Dessert",   10.00),
    ("Apple Pie",              "Dessert",    8.00),
    ("Ice Cream Sundae",       "Dessert",    7.00),
    ("Soft Drink",             "Beverage",   3.00),
    ("Iced Tea",               "Beverage",   3.00),
    ("Coffee",                 "Beverage",   3.50),
    ("Fresh Lemonade",         "Beverage",   4.00),
    ("Bottled Water",          "Beverage",   2.00),
    ("Draft Beer",             "Alcohol",    7.00),
    ("House Wine",             "Alcohol",    9.00),
    ("Margarita",              "Alcohol",   11.00),
    ("Old Fashioned",          "Alcohol",   13.00),
]

ORDER_TYPES = ["dine_in", "takeout", "delivery"]
ORDER_TYPE_WEIGHTS = [0.52, 0.31, 0.17]
PAYMENT_METHODS = ["card", "cash", "mobile_pay", "gift_card"]
PAYMENT_WEIGHTS = [0.61, 0.18, 0.16, 0.05]
STATUSES = ["completed", "cancelled", "refunded"]
STATUS_WEIGHTS = [0.94, 0.04, 0.02]

# Hour-of-day weights (open 11:00–22:00) — lunch + dinner humps
HOUR_WEIGHTS = {11: 6, 12: 12, 13: 11, 14: 6, 15: 3, 16: 4,
                17: 8, 18: 13, 19: 14, 20: 11, 21: 7, 22: 4}
# Day-of-week weights (0=Mon … 6=Sun) — weekend skew
DOW_WEIGHTS = {0: 8, 1: 8, 2: 9, 3: 10, 4: 14, 5: 16, 6: 13}

ITEMS_PER_ORDER = [1, 2, 3, 4, 5, 6]
ITEMS_PER_ORDER_P = [0.22, 0.30, 0.25, 0.13, 0.07, 0.03]
QTY_CHOICES = [1, 2, 3]
QTY_P = [0.82, 0.14, 0.04]


# ----------------------------------------------------------------------------
# BUILDERS
# ----------------------------------------------------------------------------
def build_menu_df():
    rows = []
    for i, (name, cat, price) in enumerate(MENU, start=1):
        rows.append({"menu_item_id": i, "item_name": name,
                     "category": cat, "base_price": round(price, 2)})
    return pd.DataFrame(rows)


def build_customers_df(n):
    rows = []
    # Signups span 2 yrs before the window through the end of it. A few customers
    # will therefore have a signup_date AFTER their first order — an intentional
    # wrinkle you can catch with a dbt test (order_date >= signup_date).
    earliest = START_DATE - timedelta(days=730)
    for i in range(1, n + 1):
        rows.append({
            "customer_id": i,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.numerify("###-###-####"),
            "signup_date": fake.date_between(start_date=earliest, end_date=END_DATE).isoformat(),
            "loyalty_tier": np.random.choice(["bronze", "silver", "gold"], p=[0.6, 0.3, 0.1]),
        })
    return pd.DataFrame(rows)


def sample_order_datetimes(n):
    """Sample n timestamps weighted by day-of-week and hour-of-day."""
    all_days = []
    cur = START_DATE
    while cur <= END_DATE:
        all_days.append(cur)
        cur += timedelta(days=1)
    day_w = np.array([DOW_WEIGHTS[d.weekday()] for d in all_days], dtype=float)
    day_w /= day_w.sum()
    chosen_days = np.random.choice(len(all_days), size=n, p=day_w)

    hours = np.array(list(HOUR_WEIGHTS.keys()))
    hour_w = np.array(list(HOUR_WEIGHTS.values()), dtype=float)
    hour_w /= hour_w.sum()
    chosen_hours = np.random.choice(hours, size=n, p=hour_w)

    out = []
    for di, h in zip(chosen_days, chosen_hours):
        d = all_days[di]
        out.append(datetime(d.year, d.month, d.day,
                            int(h), random.randint(0, 59), random.randint(0, 59)))
    return out


def build_orders_and_items(menu_df, customer_ids):
    # Lognormal frequency weights -> a few regulars, a long tail of one-timers
    freq_w = np.random.lognormal(mean=0.0, sigma=1.0, size=len(customer_ids))
    freq_w /= freq_w.sum()

    timestamps = sample_order_datetimes(N_ORDERS)
    timestamps.sort()

    price_lookup = dict(zip(menu_df.menu_item_id, menu_df.base_price))
    menu_ids = list(menu_df.menu_item_id)

    orders, items = [], []
    order_item_id = 1

    for oid, ts in enumerate(timestamps, start=1):
        rest = int(np.random.choice([1, 2, 3, 4], p=RESTAURANT_WEIGHTS))
        otype = random.choices(ORDER_TYPES, weights=ORDER_TYPE_WEIGHTS)[0]

        if random.random() < GUEST_ORDER_RATE:
            cust = None
        else:
            cust = int(np.random.choice(customer_ids, p=freq_w))

        order = {
            "order_id": oid,
            "restaurant_id": rest,
            "customer_id": cust,
            "order_timestamp": ts.isoformat(sep=" "),
            "order_type": otype,
            "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
            "status": random.choices(STATUSES, weights=STATUS_WEIGHTS)[0],
        }

        # transaction-time price: apply the mid-year bump
        bump = PRICE_INCREASE_FACTOR if ts.date() >= PRICE_INCREASE_DATE else 1.0

        n_items = random.choices(ITEMS_PER_ORDER, weights=ITEMS_PER_ORDER_P)[0]
        n_items = min(n_items, len(menu_ids))
        chosen = random.sample(menu_ids, k=n_items)
        order_total = 0.0
        for mid in chosen:
            qty = random.choices(QTY_CHOICES, weights=QTY_P)[0]
            unit_price = round(price_lookup[mid] * bump, 2)
            order_total += unit_price * qty
            items.append({
                "order_item_id": order_item_id,
                "order_id": oid,
                "menu_item_id": mid,
                "quantity": qty,
                "unit_price": unit_price,
            })
            order_item_id += 1

        if GENERATE_ORDER_TOTAL:
            order["order_total"] = round(order_total, 2)

        orders.append(order)

    return pd.DataFrame(orders), pd.DataFrame(items)


# ----------------------------------------------------------------------------
# MESSINESS  (only when INTRODUCE_MESSINESS = True)
#   1. ~3% of orders get inconsistent order_type / payment_method casing+whitespace
#   2. ~0.4% of orders are duplicated wholesale (breaks the order_id PK -> unique test)
#   3. ~2% of customers get a null email; ~1% get a blank-string phone
# ----------------------------------------------------------------------------
def dirty_orders(orders_df):
    df = orders_df.copy()
    n = len(df)
    idx = df.sample(frac=0.03, random_state=SEED).index
    swaps = {"dine_in": "Dine_In", "takeout": " takeout ", "delivery": "DELIVERY",
             "card": "CARD", "cash": "Cash ", "mobile_pay": "mobile pay", "gift_card": "giftcard"}
    for i in idx:
        df.at[i, "order_type"] = swaps.get(df.at[i, "order_type"], df.at[i, "order_type"])
        df.at[i, "payment_method"] = swaps.get(df.at[i, "payment_method"], df.at[i, "payment_method"])

    dupes = df.sample(frac=0.004, random_state=SEED + 1)
    df = pd.concat([df, dupes], ignore_index=True)
    return df


def dirty_customers(cust_df):
    df = cust_df.copy()
    null_email = df.sample(frac=0.02, random_state=SEED).index
    df.loc[null_email, "email"] = np.nan
    blank_phone = df.sample(frac=0.01, random_state=SEED + 1).index
    df.loc[blank_phone, "phone"] = ""
    return df


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    restaurants_df = pd.DataFrame(RESTAURANTS)
    menu_df = build_menu_df()
    customers_df = build_customers_df(N_CUSTOMERS)
    orders_df, items_df = build_orders_and_items(menu_df, list(customers_df.customer_id))

    if INTRODUCE_MESSINESS:
        customers_df = dirty_customers(customers_df)
        orders_df = dirty_orders(orders_df)

    # nullable customer_id -> keep as Int64 so it doesn't become a float in CSV
    orders_df["customer_id"] = orders_df["customer_id"].astype("Int64")

    files = {
        "restaurants.csv": restaurants_df,
        "menu_items.csv": menu_df,
        "customers.csv": customers_df,
        "orders.csv": orders_df,
        "order_items.csv": items_df,
    }
    for fname, df in files.items():
        path = os.path.join(OUTPUT_DIR, fname)
        df.to_csv(path, index=False)
        print(f"  {fname:<18} {len(df):>7,} rows")

    print(f"\nWrote {len(files)} files to ./{OUTPUT_DIR}/")
    print(f"  date range : {START_DATE} → {END_DATE}")
    print(f"  guests     : ~{GUEST_ORDER_RATE:.0%} of orders have no customer_id")
    print(f"  price bump : {PRICE_INCREASE_FACTOR:.0%} from {PRICE_INCREASE_DATE}")
    print(f"  messiness  : {'ON' if INTRODUCE_MESSINESS else 'off'}")


if __name__ == "__main__":
    main()
