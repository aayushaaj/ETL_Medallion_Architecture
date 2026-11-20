"""Generate 5 consecutive days of ycotek landing-zone CSVs for assignment 2.3.

Usage:
    python scripts/generate_data.py

Outputs to assignment_2_3/data/:
    batch_2026-11-18/ ... batch_2026-11-22/  (4 CSVs + .COMPLETE each)
    customer_seed.csv
"""
from pathlib import Path

# __file__ is .../bootcamp/assignment_2_3/scripts/generate_data.py
# SCRIPT_DIR is .../bootcamp/assignment_2_3/scripts
SCRIPT_DIR = Path(__file__).resolve().parent

# OUT is .../bootcamp/assignment_2_3/data
OUT = SCRIPT_DIR.parent / "data"

DATES = ["2026-11-18", "2026-11-19", "2026-11-20", "2026-11-21", "2026-11-22"]

ITEMS = [
    # item_id, name, subclass_id, subclass_name, unit_price, unit_cost, effective_from
    ("SKU001", "Widget A", "SUB01", "Hardware", "10.00", "6.00", "2026-11-01"),
    ("SKU002", "Widget B", "SUB01", "Hardware", "15.00", "9.00", "2026-11-01"),
    ("SKU003", "Gadget C", "SUB02", "Electronics", "25.00", "15.00", "2026-11-01"),
    ("SKU004", "Gadget D", "SUB02", "Electronics", "30.00", "18.00", "2026-11-01"),
    ("SKU005", "Tool E", "SUB01", "Hardware", "12.00", "7.00", "2026-11-01"),
]

STORES_BASE = [
    # location_id, name, district_id, district_name, region, effective_from
    ("STORE01", "Downtown Store", "DISTRICT_NORTH", "North District", "North", "2026-11-01"),
    ("STORE02", "Mall Store", "DISTRICT_NORTH", "North District", "North", "2026-11-01"),
    ("STORE03", "Airport Store", "DISTRICT_SOUTH", "South District", "South", "2026-11-01"),
]

STORES_AFTER_DAY3 = [
    ("STORE01", "Downtown Store", "DISTRICT_NORTH", "North District", "North", "2026-11-01"),
    ("STORE02", "Mall Store", "DISTRICT_SOUTH", "South District", "South", "2026-11-20"),
    ("STORE03", "Airport Store", "DISTRICT_SOUTH", "South District", "South", "2026-11-01"),
]

# pos_sales rows per day: (txn_id, line_id, item_id, loc_id, cust_id, qty, price, amount, method)
POS = {
    "2026-11-18": [
        ("TXN001", 1, "SKU001", "STORE01", "CUST01", 2, "10.00", "20.00", "CARD"),
        ("TXN001", 2, "SKU003", "STORE01", "CUST01", 1, "25.00", "25.00", "CARD"),
        ("TXN002", 1, "SKU002", "STORE02", "CUST02", 3, "15.00", "45.00", "CASH"),
        ("TXN003", 1, "SKU004", "STORE03", "CUST01", 1, "30.00", "30.00", "CARD"),
        ("TXN003", 2, "SKU005", "STORE03", "CUST01", 2, "12.00", "24.00", "CARD"),
    ],
    "2026-11-19": [
        ("TXN004", 1, "SKU001", "STORE02", "CUST02", 1, "10.00", "10.00", "CASH"),
        ("TXN005", 1, "SKU003", "STORE01", "CUST01", 2, "25.00", "50.00", "CARD"),
        ("TXN005", 2, "SKU004", "STORE01", "CUST01", 1, "30.00", "30.00", "CARD"),
        ("TXN006", 1, "SKU002", "STORE03", "CUST02", 4, "15.00", "60.00", "CARD"),
    ],
    "2026-11-20": [
        ("TXN007", 1, "", "STORE01", "CUST01", 2, "10.00", "20.00", "CARD"),    # defect: null FK
        ("TXN007", 2, "", "STORE01", "CUST01", 1, "25.00", "25.00", "CARD"),    # defect: null FK
        ("TXN008", 1, "SKU002", "STORE02", "CUST02", -1, "15.00", "-15.00", "CARD"),  # defect: neg qty
        ("TXN009", 1, "SKU999", "STORE03", "CUST01", 1, "99.99", "99.99", "CASH"),    # defect: orphan FK
        ("TXN010", 1, "SKU001", "STORE01", "CUST01", 2, "10.00", "20.00", "CARD"),
        ("TXN010", 1, "SKU001", "STORE01", "CUST01", 2, "10.00", "20.00", "CARD"),    # defect: duplicate
        ("TXN011", 1, "SKU004", "STORE02", "CUST02", 3, "30.00", "90.00", "CARD"),
        ("TXN011", 2, "SKU005", "STORE02", "CUST02", 1, "12.00", "12.00", "CARD"),
        ("TXN012", 1, "SKU003", "STORE03", "CUST01", 2, "25.00", "50.00", "CARD"),
        ("TXN012", 2, "SKU002", "STORE01", "CUST02", 1, "15.00", "15.00", "CARD"),
    ],
    "2026-11-21": [
        ("TXN013", 1, "SKU003", "STORE01", "CUST01", 2, "25.00", "50.00", "CARD"),
        ("TXN014", 1, "SKU005", "STORE02", "CUST02", 5, "12.00", "60.00", "CARD"),
        ("TXN014", 2, "SKU002", "STORE02", "CUST02", 1, "15.00", "15.00", "CARD"),
    ],
    "2026-11-22": [
        ("TXN015", 1, "SKU001", "STORE03", "CUST01", 3, "10.00", "30.00", "CARD"),
        ("TXN015", 2, "SKU004", "STORE03", "CUST01", 1, "30.00", "30.00", "CARD"),
        ("TXN016", 1, "SKU002", "STORE01", "CUST02", 2, "15.00", "30.00", "CASH"),
    ],
}

# inventory: (item_id, location_id, on_hand, on_order, unit_cost) per day
INV = {
    "2026-11-18": [
        ("SKU001", "STORE01", 100, 50, "6.00"), ("SKU001", "STORE02", 80, 40, "6.00"), ("SKU001", "STORE03", 60, 30, "6.00"),
        ("SKU002", "STORE01", 150, 75, "9.00"), ("SKU002", "STORE02", 120, 60, "9.00"), ("SKU002", "STORE03", 90, 45, "9.00"),
        ("SKU003", "STORE01", 50, 25, "15.00"), ("SKU003", "STORE02", 40, 20, "15.00"), ("SKU003", "STORE03", 30, 15, "15.00"),
        ("SKU004", "STORE01", 70, 35, "18.00"), ("SKU004", "STORE02", 55, 25, "18.00"), ("SKU004", "STORE03", 45, 20, "18.00"),
        ("SKU005", "STORE01", 200, 100, "7.00"), ("SKU005", "STORE02", 180, 90, "7.00"), ("SKU005", "STORE03", 160, 80, "7.00"),
    ],
    "2026-11-19": [
        ("SKU001", "STORE01", 98, 50, "6.00"), ("SKU001", "STORE02", 80, 40, "6.00"), ("SKU001", "STORE03", 60, 30, "6.00"),
        ("SKU002", "STORE01", 150, 75, "9.00"), ("SKU002", "STORE02", 117, 60, "9.00"), ("SKU002", "STORE03", 90, 45, "9.00"),
        ("SKU003", "STORE01", 49, 25, "15.00"), ("SKU003", "STORE02", 40, 20, "15.00"), ("SKU003", "STORE03", 30, 15, "15.00"),
        ("SKU004", "STORE01", 70, 35, "18.00"), ("SKU004", "STORE02", 55, 25, "18.00"), ("SKU004", "STORE03", 44, 20, "18.00"),
        ("SKU005", "STORE01", 200, 100, "7.00"), ("SKU005", "STORE02", 180, 90, "7.00"), ("SKU005", "STORE03", 158, 80, "7.00"),
    ],
    "2026-11-20": [
        ("SKU001", "STORE01", 98, 50, "6.00"), ("SKU001", "STORE02", 79, 40, "6.00"), ("SKU001", "STORE03", 60, 30, "6.00"),
        ("SKU002", "STORE01", 150, 75, "9.00"), ("SKU002", "STORE02", 117, 60, "9.00"), ("SKU002", "STORE03", 86, 45, "9.00"),
        ("SKU003", "STORE01", 47, 25, "15.00"), ("SKU003", "STORE02", 40, 20, "15.00"), ("SKU003", "STORE03", 30, 15, "15.00"),
        ("SKU004", "STORE01", 69, 35, "18.00"), ("SKU004", "STORE02", 55, 25, "18.00"), ("SKU004", "STORE03", 44, 20, "18.00"),
        ("SKU005", "STORE01", 200, 100, "7.00"), ("SKU005", "STORE02", 180, 90, "7.00"), ("SKU005", "STORE03", 158, 80, "7.00"),
    ],
    "2026-11-21": [
        ("SKU001", "STORE01", 96, 50, "6.00"), ("SKU001", "STORE02", 79, 40, "6.00"), ("SKU001", "STORE03", 60, 30, "6.00"),
        ("SKU002", "STORE01", 149, 75, "9.00"), ("SKU002", "STORE02", 117, 60, "9.00"), ("SKU002", "STORE03", 86, 45, "9.00"),
        ("SKU003", "STORE01", 47, 25, "15.00"), ("SKU003", "STORE02", 40, 20, "15.00"), ("SKU003", "STORE03", 28, 15, "15.00"),
        ("SKU004", "STORE01", 69, 35, "18.00"), ("SKU004", "STORE02", 52, 25, "18.00"), ("SKU004", "STORE03", 44, 20, "18.00"),
        ("SKU005", "STORE01", 200, 100, "7.00"), ("SKU005", "STORE02", 179, 90, "7.00"), ("SKU005", "STORE03", 158, 80, "7.00"),
    ],
    "2026-11-22": [
        ("SKU001", "STORE01", 96, 50, "6.00"), ("SKU001", "STORE02", 79, 40, "6.00"), ("SKU001", "STORE03", 60, 30, "6.00"),
        ("SKU002", "STORE01", 149, 75, "9.00"), ("SKU002", "STORE02", 116, 60, "9.00"), ("SKU002", "STORE03", 86, 45, "9.00"),
        ("SKU003", "STORE01", 45, 25, "15.00"), ("SKU003", "STORE02", 40, 20, "15.00"), ("SKU003", "STORE03", 28, 15, "15.00"),
        ("SKU004", "STORE01", 69, 35, "18.00"), ("SKU004", "STORE02", 52, 25, "18.00"), ("SKU004", "STORE03", 44, 20, "18.00"),
        ("SKU005", "STORE01", 200, 100, "7.00"), ("SKU005", "STORE02", 174, 90, "7.00"), ("SKU005", "STORE03", 158, 80, "7.00"),
    ],
}


def write_csv(path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        f.write(",".join(header) + "\n")
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")


def main():
    print(f"Generating data into: {OUT.absolute()}")
    for day in DATES:
        bdir = OUT / f"batch_{day}"

        # item_master
        write_csv(bdir / f"item_master_{day}.csv",
                  ["item_id", "item_name", "subclass_id", "subclass_name",
                   "unit_price", "unit_cost", "effective_from"],
                  ITEMS)

        # store_master (STORE02 district change on day 3)
        stores = STORES_AFTER_DAY3 if day >= "2026-11-20" else STORES_BASE
        write_csv(bdir / f"store_master_{day}.csv",
                  ["location_id", "location_name", "district_id",
                   "district_name", "region", "effective_from"],
                  stores)

        # pos_sales
        pos_rows = [(t, l, day, i, s, c, q, up, ea, pm)
                    for (t, l, i, s, c, q, up, ea, pm) in POS[day]]
        write_csv(bdir / f"pos_sales_{day}.csv",
                  ["transaction_id", "line_id", "transaction_date", "item_id",
                   "location_id", "customer_id", "sales_qty", "unit_price",
                   "extended_amount", "payment_method"],
                  pos_rows)

        # inventory_snapshot
        inv_rows = [(day, i, s, oh, oo, uc) for (i, s, oh, oo, uc) in INV[day]]
        write_csv(bdir / f"inventory_snapshot_{day}.csv",
                  ["snapshot_date", "item_id", "location_id",
                   "on_hand_qty", "on_order_qty", "unit_cost"],
                  inv_rows)

        # .COMPLETE sentinel
        (bdir / ".COMPLETE").touch()
        print(f"  ✓ {bdir.name}")

    # customer seed
    write_csv(OUT / "customer_seed.csv",
              ["customer_id", "customer_name", "customer_email",
               "customer_segment", "effective_from"],
              [("CUST01", "Alice Johnson", "alice@example.com", "RETAIL", "2026-11-01"),
               ("CUST02", "Bob Smith", "bob@example.com", "WHOLESALE", "2026-11-01")])
    print(f"  ✓ customer_seed.csv")
    print(f"\nDone. {len(DATES)} batches × 4 files + customer seed + 5 .COMPLETE sentinels.")


if __name__ == "__main__":
    main()