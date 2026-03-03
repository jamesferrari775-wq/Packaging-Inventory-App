from __future__ import annotations

import argparse
import csv
import html
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional


DEFAULT_CONFIG = {
    "target_days_of_cover": 21,
    "default_sales_window_days": 30,
    "inventory_header": {
        "sku": "SKU",
        "name": "Name",
        "unit_type": "Unit Type",
        "available_qty": "Available Quantity",
        "incoming_qty": "Incoming Quantity",
        "threshold_min": "Inventory Threshold Min",
        "category": "Category",
        "subcategory": "Subcategory",
        "group": "Group",
    },
    "sales_header_aliases": {
        "sku": ["SKU", "Item SKU", "Product SKU", "Variant SKU"],
        "qty": [
            "Quantity",
            "Qty",
            "Quantity Sold",
            "Order Quantity",
            "Units Sold",
        ],
        "date": ["Order Date", "Date", "Sale Date", "Invoice Date"],
    },
    "score_weights": {
        "coverage_gap": 70,
        "sales_velocity": 20,
        "below_threshold": 10,
    },
}


RETAIL_UNIT_SUBCATEGORIES = {
    "3 5g jar",
    "3 5g mylar",
    "14g mylar",
    "1g cart",
    "1g aio cart",
}


@dataclass
class InventoryItem:
    sku: str
    name: str
    unit_type: str
    category: str
    subcategory: str
    group: str
    list_qty: float
    available_qty: float
    incoming_qty: float
    threshold_min: float
    testing_status: str
    strain_key: str


@dataclass
class SalesItem:
    sku: str
    units_sold: float


@dataclass
class SourceCandidate:
    source_type: str
    sku: str
    name: str
    available_qty: float
    strain_key: str
    testing_status: str


def parse_testing_status(path: Optional[Path]) -> Dict[str, str]:
    if path is None or not path.exists():
        return {}
    rows = read_csv_rows(path)
    if not rows:
        return {}

    header = rows[0]
    sku_col = resolve_column(header, ["SKU", "Item SKU", "Product SKU"])
    status_col = resolve_column(header, ["Testing Status", "Status", "Test Status"])
    if not sku_col or not status_col:
        return {}

    statuses: Dict[str, str] = {}
    for raw in rows[1:]:
        if not any(cell.strip() for cell in raw):
            continue
        row = dict(zip(header, raw))
        sku = row.get(sku_col, "").strip()
        status = row.get(status_col, "").strip()
        if sku:
            statuses[sku] = status
    return statuses


def safe_float(value: Optional[str]) -> float:
    if value is None:
        return 0.0
    text = str(value).strip().replace(",", "")
    if text == "":
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def find_header_index(rows: List[List[str]], expected_headers: Iterable[str]) -> int:
    expected = {h.strip() for h in expected_headers}
    for idx, row in enumerate(rows):
        header_set = {c.strip() for c in row}
        if expected.issubset(header_set):
            return idx
    raise ValueError("Unable to find a valid header row in CSV.")


def read_csv_rows(path: Path) -> List[List[str]]:
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        return list(csv.reader(file))


def parse_inventory(path: Path, config: dict) -> Dict[str, InventoryItem]:
    rows = read_csv_rows(path)
    if not rows:
        return {}

    # Distru Packages export format (package-level rows with testing state)
    if "Distru Product" in rows[0] and "Lab Testing State" in rows[0]:
        header = rows[0]
        aggregated: Dict[str, InventoryItem] = {}

        for raw in rows[1:]:
            if not any(cell.strip() for cell in raw):
                continue
            row = dict(zip(header, raw))
            status = normalize_text(row.get("Status", ""))
            metrc_status = normalize_text(row.get("Metrc Status", ""))
            if status and status != "active":
                continue
            if metrc_status and metrc_status != "active":
                continue

            name = row.get("Distru Product", "").strip()
            if not name:
                continue

            sku = name
            category = row.get("Category", "").strip()
            subcategory = row.get("Subcategory", "").strip()
            unit_type = row.get("Unit Type", "").strip()
            available_qty = safe_float(row.get("Available Quantity"))
            list_qty = safe_float(row.get("Quantity"))
            if list_qty <= 0:
                list_qty = available_qty + safe_float(row.get("Assembling Quantity"))
            incoming_qty = safe_float(row.get("Assembling Quantity"))
            testing = row.get("Lab Testing State", "").strip()
            testing_norm = normalize_text(testing)
            is_test_passed = testing_norm in {"testpassed", "test passed"}

            if sku not in aggregated:
                aggregated[sku] = InventoryItem(
                    sku=sku,
                    name=name,
                    unit_type=unit_type,
                    category=category,
                    subcategory=subcategory,
                    group="",
                    list_qty=list_qty,
                    available_qty=available_qty if is_test_passed else 0.0,
                    incoming_qty=incoming_qty,
                    threshold_min=0.0,
                    testing_status=testing,
                    strain_key=canonicalize_strain(
                        name,
                        row.get("Production Strain", "").strip(),
                        row.get("Trim Strain", "").strip(),
                        row.get("STRAIN", "").strip(),
                    ),
                )
            else:
                aggregated[sku].list_qty += list_qty
                aggregated[sku].available_qty += available_qty if is_test_passed else 0.0
                aggregated[sku].incoming_qty += incoming_qty
                current = normalize_text(aggregated[sku].testing_status)
                if current not in {"testpassed", "test passed"} and is_test_passed:
                    aggregated[sku].testing_status = "TestPassed"
                if not aggregated[sku].strain_key:
                    aggregated[sku].strain_key = canonicalize_strain(
                        name,
                        row.get("Production Strain", "").strip(),
                        row.get("Trim Strain", "").strip(),
                        row.get("STRAIN", "").strip(),
                    )

        return aggregated

    fields = config["inventory_header"]
    required = [fields["sku"], fields["name"], fields["available_qty"], fields["incoming_qty"]]
    header_idx = find_header_index(rows, required)

    parsed: Dict[str, InventoryItem] = {}
    header = rows[header_idx]
    for raw in rows[header_idx + 1 :]:
        if not any(cell.strip() for cell in raw):
            continue
        row = dict(zip(header, raw))
        sku = row.get(fields["sku"], "").strip()
        if not sku:
            continue
        parsed[sku] = InventoryItem(
            sku=sku,
            name=row.get(fields["name"], "").strip(),
            unit_type=row.get(fields["unit_type"], "").strip(),
            category=row.get(fields["category"], "").strip(),
            subcategory=row.get(fields["subcategory"], "").strip(),
            group=row.get(fields["group"], "").strip(),
            list_qty=(safe_float(row.get("Quantity")) if safe_float(row.get("Quantity")) > 0 else safe_float(row.get(fields["available_qty"])) + safe_float(row.get(fields["incoming_qty"]))),
            available_qty=safe_float(row.get(fields["available_qty"])),
            incoming_qty=safe_float(row.get(fields["incoming_qty"])),
            threshold_min=safe_float(row.get(fields["threshold_min"])),
            testing_status="",
            strain_key=canonicalize_strain(row.get(fields["name"], "").strip()),
        )
    return parsed


def resolve_column(header: List[str], aliases: List[str]) -> Optional[str]:
    normalized = {h.strip().lower(): h for h in header}
    for candidate in aliases:
        found = normalized.get(candidate.strip().lower())
        if found:
            return found
    return None


def parse_date(value: str) -> Optional[datetime]:
    value = value.strip()
    if not value:
        return None
    formats = [
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def parse_sales(path: Path, config: dict) -> tuple[Dict[str, SalesItem], int]:
    rows = read_csv_rows(path)
    aliases = config["sales_header_aliases"]

    header_idx = -1
    for i, row in enumerate(rows):
        if not row:
            continue
        sku_col = resolve_column(row, aliases["sku"])
        qty_col = resolve_column(row, aliases["qty"])
        if sku_col and qty_col:
            header_idx = i
            break

    if header_idx == -1:
        raise ValueError(
            "Sales CSV does not contain SKU + Quantity columns. "
            "Use a Distru sales export with item-level lines (SKU and quantity sold)."
        )

    header = rows[header_idx]
    sku_col = resolve_column(header, aliases["sku"])
    qty_col = resolve_column(header, aliases["qty"])
    date_col = resolve_column(header, aliases["date"])

    if not sku_col or not qty_col:
        raise ValueError("Sales CSV missing required SKU/quantity columns.")

    sales: Dict[str, SalesItem] = {}
    min_date: Optional[datetime] = None
    max_date: Optional[datetime] = None

    for raw in rows[header_idx + 1 :]:
        if not any(cell.strip() for cell in raw):
            continue
        row = dict(zip(header, raw))
        sku = row.get(sku_col, "").strip()
        qty = safe_float(row.get(qty_col))
        if not sku or qty == 0:
            continue

        if date_col:
            dt = parse_date(row.get(date_col, ""))
            if dt:
                min_date = dt if min_date is None else min(min_date, dt)
                max_date = dt if max_date is None else max(max_date, dt)

        current = sales.get(sku)
        if current is None:
            sales[sku] = SalesItem(sku=sku, units_sold=qty)
        else:
            current.units_sold += qty

    if min_date and max_date and max_date >= min_date:
        window_days = max(1, (max_date.date() - min_date.date()).days + 1)
    else:
        window_days = int(config["default_sales_window_days"])

    return sales, window_days


def compute_priority_rows(
    inventory: Dict[str, InventoryItem],
    sales: Dict[str, SalesItem],
    sales_window_days: int,
    config: dict,
) -> List[dict]:
    target_days = float(config["target_days_of_cover"])
    weights = config["score_weights"]

    max_avg_daily = 0.0
    sku_to_avg: Dict[str, float] = {}
    for sku, sales_item in sales.items():
        avg_daily = sales_item.units_sold / max(1, sales_window_days)
        sku_to_avg[sku] = avg_daily
        max_avg_daily = max(max_avg_daily, avg_daily)

    rows: List[dict] = []
    for rank_sku, inv in inventory.items():
        need_type = get_need_type(inv)
        is_bulk_no_sales = need_type == "" and is_bulk_option(inv)
        if need_type == "" and not is_bulk_no_sales:
            continue

        if len(sales) == 0 and inv.available_qty <= 0 and inv.incoming_qty <= 0:
            continue

        sold = sales.get(rank_sku).units_sold if rank_sku in sales else 0.0
        avg_daily = sku_to_avg.get(rank_sku, 0.0)
        sku_has_sales = avg_daily > 0
        days_cover = (inv.available_qty / avg_daily) if avg_daily > 0 else 9999.0

        total_on_hand_soon = inv.available_qty + inv.incoming_qty
        target_units = avg_daily * target_days
        if sku_has_sales:
            reorder_qty = max(0.0, target_units - total_on_hand_soon)
        else:
            reorder_qty = max(0.0, inv.threshold_min - total_on_hand_soon)
            if reorder_qty == 0 and inv.available_qty <= 0:
                reorder_qty = 1.0

        coverage_gap = max(0.0, min(1.0, (target_days - days_cover) / target_days)) if avg_daily > 0 else 0.0
        velocity = (avg_daily / max_avg_daily) if max_avg_daily > 0 else 0.0
        below_threshold = 1.0 if (inv.threshold_min > 0 and inv.available_qty < inv.threshold_min) else 0.0
        unavailable = 1.0 if inv.available_qty <= 0 else 0.0

        if sku_has_sales:
            score = (
                coverage_gap * weights["coverage_gap"]
                + velocity * weights["sales_velocity"]
                + below_threshold * weights["below_threshold"]
            )
        else:
            threshold_gap = 0.0
            if inv.threshold_min > 0:
                threshold_gap = max(0.0, min(1.0, (inv.threshold_min - inv.available_qty) / inv.threshold_min))
            incoming_relief = 1.0 if inv.incoming_qty > 0 else 0.0
            score = (unavailable * 70.0) + (threshold_gap * 25.0) + (below_threshold * 10.0) - (incoming_relief * 5.0)
            score = max(0.0, min(100.0, score))

        if reorder_qty <= 0 and inv.available_qty >= inv.threshold_min:
            priority = "Low"
        elif days_cover < 7 or score >= 70:
            priority = "Critical"
        elif days_cover < 14 or score >= 45:
            priority = "High"
        elif days_cover < target_days or score >= 25:
            priority = "Medium"
        else:
            priority = "Low"

        if len(sales) == 0 and inv.available_qty > 0 and not is_bulk_no_sales:
            continue

        rows.append(
            {
                "Priority": priority,
                "SKU": inv.sku,
                "Name": inv.name,
                "Need Type": need_type if need_type else "Bulk",
                "Unit Type": inv.unit_type,
                "Category": inv.category,
                "Subcategory": inv.subcategory,
                "List Qty": round(inv.list_qty, 2),
                "Available Qty": round(inv.available_qty, 2),
                "Reserved Qty": round(max(0.0, inv.list_qty - inv.available_qty), 2),
                "Incoming Qty": round(inv.incoming_qty, 2),
                "Threshold Min": round(inv.threshold_min, 2),
                "Sales Window Days": sales_window_days,
                "Units Sold": round(sold, 2),
                "Avg Daily Sales": round(avg_daily, 4),
                "Days of Cover": round(days_cover, 1) if days_cover < 9999 else "",
                "Target Days of Cover": target_days,
                "Recommended Reorder Qty": math.ceil(reorder_qty),
                "Urgency Score": round(score, 2),
                "Unavailable": "Yes" if unavailable > 0 else "No",
            }
        )

    priority_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    rows.sort(
        key=lambda r: (
            priority_order.get(r["Priority"], 99),
            -float(r["Urgency Score"]),
            -float(r["Recommended Reorder Qty"]),
        )
    )
    for idx, row in enumerate(rows, start=1):
        row["Priority Rank"] = idx
    return rows


def normalize_text(value: str) -> str:
    ascii_text = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]+", " ", ascii_text.lower())).strip()


def clean_strain_text(value: str) -> str:
    text = normalize_text(value)
    if not text:
        return ""

    removable_phrases = [
        "srene",
        "vybz",
        "level",
        "bulk flower",
        "bulk shake trim",
        "bulk trim",
        "preroll material",
        "raw distillate",
        "formulated distillate",
        "distillate finished product",
        "1g aio cart",
        "1g cart",
        "pre roll",
        "pre rolled",
        "5 pack",
        "5pk",
        "3 5 grams",
        "3 5g",
        "14g",
        "1 2 ounce",
        "half ounce",
        "jar",
        "mylar",
    ]
    for phrase in removable_phrases:
        text = text.replace(phrase, " ")

    tokens = []
    remove_tokens = {
        "bud",
        "trim",
        "flower",
        "smalls",
        "material",
        "distillate",
        "cart",
        "aio",
        "pre",
        "roll",
        "grams",
        "gram",
        "pack",
        "pk",
        "jar",
        "mylar",
    }
    for token in text.split():
        if token in remove_tokens:
            continue
        if re.match(r"^\d+(?:\.\d+)?(?:g|mg|pk)?$", token):
            continue
        tokens.append(token)

    return " ".join(tokens).strip()


def is_excluded_item(inv: InventoryItem) -> bool:
    text = normalize_text(f"{inv.name} {inv.group} {inv.subcategory}")
    excluded_markers = ["tester", "promo", "display"]
    return any(marker in text for marker in excluded_markers)


def is_metrc_tracked_cannabis(inv: InventoryItem) -> bool:
    if is_excluded_item(inv):
        return False

    category = normalize_text(inv.category)
    subcategory = normalize_text(inv.subcategory)

    excluded_categories = [
        "packaging supplies",
        "services",
        "merchandise",
        "seeds",
        "hemp derived thc",
        "ingredients",
    ]
    if any(term in category for term in excluded_categories):
        return False

    allowed_signals = [
        "marijuana flowers",
        "small popcorn buds",
        "pre rolled cigs joints",
        "non solvent based concentrate",
        "solvent based concentrate",
        "capsule tablet",
        "infused edible",
        "tinctures",
        "shake trim",
    ]
    return any(signal in category or signal in subcategory for signal in allowed_signals)


def get_need_type(inv: InventoryItem) -> str:
    subcategory = normalize_text(inv.subcategory)
    category = normalize_text(inv.category)
    if subcategory in {"1g cart", "1g aio cart"}:
        return "Cart"
    if subcategory in RETAIL_UNIT_SUBCATEGORIES:
        return "Unit"
    if "pre rolled cigs joints" in category or "pre roll" in subcategory:
        return "Preroll"
    return ""


def grams_per_unit_for_station(station: str, subcategory: str = "", name: str = "") -> float:
    station_norm = normalize_text(station)
    details = normalize_text(f"{subcategory} {name}")

    if station_norm == "cart":
        return 1.0

    if station_norm == "unit":
        smalls_markers = ["smalls", "small popcorn buds"]
        if any(marker in details for marker in smalls_markers):
            return 14.0
        fourteen_markers = ["14g", "14 g", "1 2 ounce", "half ounce"]
        if any(marker in details for marker in fourteen_markers):
            return 14.0
        return 3.5

    if station_norm == "preroll":
        five_pack_markers = ["5pk", "5 pk", "5pack", "5 pack", "0 7g", "0.7g", ".7g"]
        if any(marker in details for marker in five_pack_markers):
            return 3.5
        return 1.0

    return 1.0


def convert_grams_to_units(qty_grams: float, station: str, subcategory: str = "", name: str = "") -> int:
    grams_per_unit = grams_per_unit_for_station(station, subcategory=subcategory, name=name)
    if grams_per_unit <= 0:
        return 0
    return int(math.floor(qty_grams / grams_per_unit))


def inventory_subtype_label(station: str, subcategory: str = "", name: str = "") -> str:
    station_norm = normalize_text(station)
    details = normalize_text(f"{subcategory} {name}")

    if station_norm == "unit":
        return "14g" if grams_per_unit_for_station("Unit", subcategory=subcategory, name=name) == 14.0 else "3.5g"

    if station_norm == "cart":
        return "1g AIO" if "aio" in details else "1g Cart"

    if station_norm == "preroll":
        return "5pk (.7g)" if grams_per_unit_for_station("Preroll", subcategory=subcategory, name=name) == 3.5 else "1g Single"

    return "Other"


def format_packaged_breakdown(station: str, counts: Dict[str, int]) -> str:
    if normalize_text(station) == "unit":
        return f"3.5g: {counts.get('3.5g', 0)} | 14g: {counts.get('14g', 0)}"
    if normalize_text(station) == "cart":
        return f"1g Cart: {counts.get('1g Cart', 0)} | 1g AIO: {counts.get('1g AIO', 0)}"
    if normalize_text(station) == "preroll":
        return f"1g Single: {counts.get('1g Single', 0)} | 5pk (.7g): {counts.get('5pk (.7g)', 0)}"
    non_zero = [f"{k}: {v}" for k, v in counts.items() if v]
    return " | ".join(non_zero)


def parse_breakdown_counts(breakdown: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for segment in str(breakdown or "").split("|"):
        part = segment.strip()
        if ":" not in part:
            continue
        label, value = part.split(":", 1)
        number = safe_float(value.strip())
        counts[label.strip()] = int(number)
    return counts


def get_required_subtypes(station: str) -> List[str]:
    station_norm = normalize_text(station)
    if station_norm == "unit":
        return ["3.5g", "14g"]
    if station_norm == "cart":
        return ["1g Cart", "1g AIO"]
    if station_norm == "preroll":
        return ["1g Single", "5pk (.7g)"]
    return []


def find_missing_required_subtypes(station: str, breakdown: str) -> List[str]:
    required = get_required_subtypes(station)
    if not required:
        return []
    counts = parse_breakdown_counts(breakdown)
    return [label for label in required if counts.get(label, 0) <= 0]


def packaging_recommendation_for_zero_inventory(station: str, need_name: str, breakdown: str) -> str:
    counts = parse_breakdown_counts(breakdown)
    required = get_required_subtypes(station)
    if not required:
        return ""

    target_option = inventory_subtype_label(station, name=need_name)
    if target_option not in required:
        return ""
    if counts.get(target_option, 0) > 0:
        return ""

    alternatives = [(label, count) for label, count in counts.items() if label != target_option and count > 0]
    if not alternatives:
        return ""

    best_option, best_count = max(alternatives, key=lambda item: item[1])
    return (
        f"Package more {target_option} options now; use available {best_option} inventory "
        f"({best_count} units) for this strain"
    )


def packaging_recommendation_for_subtype_gap(
    station: str,
    need_name: str,
    breakdown: str,
    missing_types: List[str],
) -> str:
    if not missing_types:
        return ""

    counts = parse_breakdown_counts(breakdown)
    station_norm = normalize_text(station)
    need_name_norm = normalize_text(need_name)
    is_smalls_unit = station_norm == "unit" and (
        "smalls" in need_name_norm or "small popcorn buds" in need_name_norm
    )

    if is_smalls_unit:
        if "14g" not in missing_types:
            return ""
        target_option = "14g"
    else:
        need_option = inventory_subtype_label(station, name=need_name)
        target_option = need_option if need_option in missing_types else missing_types[0]

    alternatives = [(label, count) for label, count in counts.items() if label not in missing_types and count > 0]
    if not alternatives:
        return ""

    best_option, best_count = max(alternatives, key=lambda item: item[1])
    return (
        f"Package {target_option} options next; use available {best_option} inventory "
        f"({best_count} units) for this strain"
    )


def filter_inventory_for_priority(inventory: Dict[str, InventoryItem]) -> Dict[str, InventoryItem]:
    return {sku: item for sku, item in inventory.items() if is_metrc_tracked_cannabis(item)}


def filter_inventory_for_needs(inventory: Dict[str, InventoryItem]) -> Dict[str, InventoryItem]:
    return {sku: item for sku, item in inventory.items() if get_need_type(item) != ""}


def is_bulk_option(inv: InventoryItem) -> bool:
    return is_flower_source(inv) or is_distillate_source(inv) or is_preroll_source(inv)


def extract_key(name: str) -> str:
    text = normalize_text(name)
    parts = [p.strip() for p in re.split(r"\||-", text) if p.strip()]
    blacklist = {
        "srene",
        "vybz",
        "level",
        "bud",
        "trim",
        "distillate",
        "cart",
        "aio",
        "pre roll",
        "pre rolled",
        "pre",
        "roll",
        "jar",
        "grams",
        "gram",
        "1g",
        "3 5",
        "14",
        "half ounce",
        "eighth",
        "unit",
    }
    for part in reversed(parts):
        p = part.strip()
        if len(p) < 3:
            continue
        if p in blacklist:
            continue
        if p.isdigit():
            continue
        return p
    return text


def canonicalize_strain(
    name: str,
    production_strain: str = "",
    trim_strain: str = "",
    strain: str = "",
) -> str:
    preferred_values = [production_strain, trim_strain, strain]
    for preferred in preferred_values:
        cleaned = clean_strain_text(preferred or "")
        if cleaned:
            return cleaned

    cleaned_name = clean_strain_text(name)
    if cleaned_name:
        return cleaned_name
    return clean_strain_text(extract_key(name))


def is_distillate_source(inv: InventoryItem) -> bool:
    combined = normalize_text(f"{inv.category} {inv.subcategory} {inv.name}")
    unit_type = normalize_text(inv.unit_type)
    if unit_type not in {"gram", "g"}:
        return False
    if "distillate" not in combined:
        return False
    return "bulk" in combined or "raw distillate" in combined or "formulated distillate" in combined


def is_flower_source(inv: InventoryItem) -> bool:
    combined = normalize_text(f"{inv.category} {inv.subcategory} {inv.name}")
    unit_type = normalize_text(inv.unit_type)
    if unit_type not in {"gram", "g"}:
        return False
    is_bulk_flower = "bulk flower" in combined
    is_bulk_smalls = "bulk smalls" in combined or "small popcorn buds" in combined
    return is_bulk_flower or is_bulk_smalls


def is_preroll_source(inv: InventoryItem) -> bool:
    combined = normalize_text(f"{inv.category} {inv.subcategory} {inv.name}")
    unit_type = normalize_text(inv.unit_type)
    if unit_type not in {"gram", "g"}:
        return False
    return "trim" in combined or "preroll material" in combined


def build_source_candidates(
    inventory: Dict[str, InventoryItem],
) -> tuple[List[SourceCandidate], List[SourceCandidate], List[SourceCandidate]]:
    flower_sources: List[SourceCandidate] = []
    distillate_sources: List[SourceCandidate] = []
    preroll_sources: List[SourceCandidate] = []
    for inv in inventory.values():
        if inv.available_qty <= 0:
            continue
        strain_key = inv.strain_key or normalize_text(extract_key(inv.name))
        if is_distillate_source(inv):
            distillate_sources.append(
                SourceCandidate("distillate", inv.sku, inv.name, inv.available_qty, strain_key, inv.testing_status)
            )
        if is_flower_source(inv):
            flower_sources.append(SourceCandidate("flower", inv.sku, inv.name, inv.available_qty, strain_key, inv.testing_status))
        if is_preroll_source(inv):
            preroll_sources.append(SourceCandidate("preroll", inv.sku, inv.name, inv.available_qty, strain_key, inv.testing_status))
    distillate_sources.sort(key=lambda s: -s.available_qty)
    flower_sources.sort(key=lambda s: -s.available_qty)
    preroll_sources.sort(key=lambda s: -s.available_qty)
    return flower_sources, distillate_sources, preroll_sources


def select_source(need_key: str, sources: List[SourceCandidate]) -> tuple[Optional[SourceCandidate], str]:
    if not sources:
        return None, "None"
    normalized_key = normalize_text(need_key)
    exact = [s for s in sources if normalize_text(s.strain_key) == normalized_key]
    if exact:
        return exact[0], "Exact"
    contains = [
        s
        for s in sources
        if normalized_key in normalize_text(s.name)
        or normalize_text(s.strain_key) in normalized_key
    ]
    if contains:
        return contains[0], "Similar"
    return None, "NoMatch"


def build_production_rows(
    priority_rows: List[dict],
    needs_inventory: Dict[str, InventoryItem],
    source_inventory: Dict[str, InventoryItem],
    testing_status: Dict[str, str],
    top_n: int,
) -> List[dict]:
    flower_sources, distillate_sources, preroll_sources = build_source_candidates(source_inventory)
    production_rows: List[dict] = []

    for row in priority_rows:
        if float(row["Recommended Reorder Qty"]) <= 0 and float(row["Available Qty"]) <= 0:
            continue

        sku = str(row["SKU"])
        inv = needs_inventory.get(sku)
        if not inv:
            continue

        need_key = inv.strain_key or normalize_text(extract_key(inv.name))
        need_type = get_need_type(inv)
        if need_type == "":
            continue
        source_type_needed = ""
        source: Optional[SourceCandidate] = None
        match_type = "N/A"

        if need_type == "Cart":
            source_type_needed = "Distillate"
            source, match_type = select_source(need_key, distillate_sources)
        elif need_type == "Unit":
            source_type_needed = "Flower/Smalls"
            source, match_type = select_source(need_key, flower_sources)
        elif need_type == "Preroll":
            source_type_needed = "Trim/Preroll Material"
            source, match_type = select_source(need_key, preroll_sources)

        if source_type_needed == "":
            continue

        source_available = round(source.available_qty, 2) if source else 0.0
        fillable_from_source = source is not None and source_available > 0
        source_testing = source.testing_status if source else ""
        source_testing_norm = normalize_text(source_testing)
        source_test_passed = source_testing_norm in {"test passed", "testpassed"}
        override_testing = testing_status.get(sku, "")
        if override_testing:
            source_test_passed = normalize_text(override_testing) in {"test passed", "testpassed"}
        scheduling_ready = source_test_passed and fillable_from_source
        if scheduling_ready:
            blocked_reason = ""
        elif not source:
            blocked_reason = "No matching source strain"
        elif not fillable_from_source:
            blocked_reason = "Source qty unavailable"
        elif not source_test_passed:
            blocked_reason = "Source testing not passed"
        else:
            blocked_reason = "Not schedulable"

        production_rows.append(
            {
                "Priority Rank": row["Priority Rank"],
                "Priority": row["Priority"],
                "Need SKU": row["SKU"],
                "Need Name": row["Name"],
                "Need Type": need_type,
                "Unavailable": row["Unavailable"],
                "Need List Qty": row.get("List Qty", row["Available Qty"]),
                "Need Available Qty": row["Available Qty"],
                "Need Reserved Qty": row.get("Reserved Qty", 0),
                "Need On Hand Qty": row["Available Qty"],
                "Recommended Qty": row["Recommended Reorder Qty"],
                "Matched Strain": need_key,
                "Fillable From Source": "Yes" if fillable_from_source else "No",
                "Source Rule": f"{source_type_needed} only" if source_type_needed else "No source rule",
                "Suggested Source SKU": source.sku if source else "",
                "Suggested Source Name": source.name if source else "",
                "Source Available Qty": source_available,
                "Match Type": match_type,
                "Testing Status": source_testing if source_testing else "NOT PROVIDED",
                "Scheduling Ready": "Yes" if scheduling_ready else "No",
                "Blocked Reason": blocked_reason,
            }
        )

    production_rows.sort(key=lambda r: (int(r["Priority Rank"]), str(r["Need SKU"])))
    if top_n > 0:
        return production_rows[:top_n]
    return production_rows


def write_csv(path: Path, rows: List[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = ["Priority Rank"] + [h for h in rows[0].keys() if h != "Priority Rank"]
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def write_station_outputs(base_dir: Path, production_rows: List[dict]) -> None:
    station_map = {
        "Cart": "cart_queue.csv",
        "Unit": "unit_queue.csv",
        "Preroll": "preroll_queue.csv",
    }

    base_dir.mkdir(parents=True, exist_ok=True)
    for need_type, filename in station_map.items():
        queue_rows = [
            row
            for row in production_rows
            if str(row.get("Need Type", "")) == need_type and str(row.get("Scheduling Ready", "")) == "Yes"
        ]
        queue_path = base_dir / filename
        if queue_rows:
            write_csv(queue_path, queue_rows)
        elif queue_path.exists():
            queue_path.unlink()


def write_station_todo_outputs(base_dir: Path, todo_rows: List[dict]) -> None:
    station_files = {
        "Cart": "cart_todo.csv",
        "Unit": "unit_todo.csv",
        "Preroll": "preroll_todo.csv",
    }

    base_dir.mkdir(parents=True, exist_ok=True)
    for station, filename in station_files.items():
        station_rows = [row for row in todo_rows if str(row.get("Station", "")) == station]
        output_path = base_dir / filename
        if station_rows:
            write_csv(output_path, station_rows)
        elif output_path.exists():
            output_path.unlink()


def write_station_tile_views(base_dir: Path, todo_rows: List[dict], top_n: int = 10) -> None:
    stations = ["Cart", "Unit", "Preroll"]
    station_file_map = {
        "Cart": "cart_station_tiles.html",
        "Unit": "unit_station_tiles.html",
        "Preroll": "preroll_station_tiles.html",
    }
    base_dir.mkdir(parents=True, exist_ok=True)

    grouped: Dict[str, List[dict]] = {}
    for station in stations:
        station_rows = [row for row in todo_rows if str(row.get("Station", "")) == station]
        opportunity_rows = [row for row in station_rows if str(row.get("Blocked Reason", "")).startswith("New ")]
        grouped[station] = (opportunity_rows if opportunity_rows else station_rows)[:top_n]

    css = """
    body { font-family: Arial, sans-serif; margin: 20px; background: #f7f7f9; color: #1f2937; }
    h1 { margin: 0 0 16px 0; font-size: 28px; }
    h2 { margin: 24px 0 10px 0; font-size: 22px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); gap: 12px; }
    .tile { background: #ffffff; border: 1px solid #e5e7eb; border-radius: 10px; padding: 14px; }
    .row { margin: 4px 0; font-size: 14px; }
    .title { font-size: 16px; font-weight: 700; margin-bottom: 6px; }
    .empty { background: #ffffff; border: 1px dashed #d1d5db; border-radius: 10px; padding: 12px; color: #6b7280; }
    """

    def render_station_section(station: str, rows: List[dict]) -> str:
        if not rows:
            return f"<h2>{html.escape(station)} (Top {top_n})</h2><div class='empty'>No items available.</div>"

        cards: List[str] = []
        for row in rows:
            packaged_breakdown = str(row.get("Packaged Unit Breakdown", "")).strip()
            packaged_units = str(row.get("Packaged Units In Inventory", "")).strip()
            inventory_units_summary = packaged_breakdown if packaged_breakdown else (packaged_units if packaged_units else "0")

            potential_total_units = str(row.get("Potential Total Units", "")).strip()
            potential_unit_breakdown = str(row.get("Potential Unit Breakdown", "")).strip()
            bulk_potential_summary = (
                potential_unit_breakdown if potential_unit_breakdown else (potential_total_units if potential_total_units else "")
            )

            cards.append(
                """
                <div class='tile'>
                    <div class='title'>#{todo_rank} · {name}</div>
                    <div class='row'><strong>Need SKU:</strong> {sku}</div>
                    <div class='row'><strong>Source:</strong> {source}</div>
                    <div class='row'><strong>Inventory Units:</strong> {inventory_units_summary}</div>
                    <div class='row'><strong>Bulk Potential Units:</strong> {bulk_potential_summary}</div>
                    <div class='row'><strong>Next Action:</strong> {next_action}</div>
                </div>
                """.format(
                    todo_rank=html.escape(str(row.get("ToDo Rank", ""))),
                    name=html.escape(str(row.get("Need Name", ""))),
                    sku=html.escape(str(row.get("Need SKU", ""))),
                    source=html.escape(str(row.get("Suggested Source Name", ""))),
                    inventory_units_summary=html.escape(inventory_units_summary),
                    bulk_potential_summary=html.escape(bulk_potential_summary),
                    next_action=html.escape(str(row.get("Next Action", ""))),
                )
            )

        return f"<h2>{html.escape(station)} (Top {top_n})</h2><div class='grid'>{''.join(cards)}</div>"

    sections = "".join(render_station_section(station, grouped[station]) for station in stations)
    document = f"""
    <!doctype html>
    <html lang='en'>
    <head>
      <meta charset='utf-8' />
      <meta name='viewport' content='width=device-width, initial-scale=1' />
      <title>Station To-Do Tiles</title>
      <style>{css}</style>
    </head>
    <body>
      <h1>Station To-Do Tiles</h1>
      {sections}
    </body>
    </html>
    """

    (base_dir / "station_todo_tiles.html").write_text(document, encoding="utf-8")

    for station in stations:
        station_section = render_station_section(station, grouped[station])
        station_document = f"""
    <!doctype html>
    <html lang='en'>
    <head>
      <meta charset='utf-8' />
      <meta name='viewport' content='width=device-width, initial-scale=1' />
      <title>{html.escape(station)} Station To-Do Tiles</title>
      <style>{css}</style>
    </head>
    <body>
      <h1>{html.escape(station)} Station To-Do Tiles</h1>
      {station_section}
    </body>
    </html>
    """
        filename = station_file_map[station]
        (base_dir / filename).write_text(station_document, encoding="utf-8")


def build_todo_rows(
    production_rows: List[dict],
    priority_rows: List[dict],
    needs_inventory: Dict[str, InventoryItem],
) -> List[dict]:
    todo_rows: List[dict] = []

    packaged_inventory_by_station_strain: Dict[tuple[str, str], List[str]] = {}
    packaged_units_total_by_station_strain: Dict[tuple[str, str], int] = {}
    packaged_units_breakdown_by_station_strain: Dict[tuple[str, str], Dict[str, int]] = {}
    for inv in needs_inventory.values():
        station = get_need_type(inv)
        if station not in {"Cart", "Unit", "Preroll"}:
            continue
        if inv.available_qty <= 0:
            continue
        strain = normalize_text(inv.strain_key)
        if not strain:
            continue
        summary = f"{inv.name}: {round(inv.available_qty, 2)}"
        key = (station, strain)
        if key not in packaged_inventory_by_station_strain:
            packaged_inventory_by_station_strain[key] = []
        packaged_inventory_by_station_strain[key].append(summary)

        if key not in packaged_units_total_by_station_strain:
            packaged_units_total_by_station_strain[key] = 0
        if key not in packaged_units_breakdown_by_station_strain:
            packaged_units_breakdown_by_station_strain[key] = {}

        converted_units = int(math.floor(inv.available_qty))
        packaged_units_total_by_station_strain[key] += converted_units

        label = inventory_subtype_label(station, subcategory=inv.subcategory, name=inv.name)
        packaged_units_breakdown_by_station_strain[key][label] = (
            packaged_units_breakdown_by_station_strain[key].get(label, 0) + converted_units
        )

    def score(row: dict) -> int:
        ready = str(row.get("Scheduling Ready", "")) == "Yes"
        unavailable = str(row.get("Unavailable", "")) == "Yes"
        blocked = str(row.get("Blocked Reason", ""))
        on_hand = float(row.get("Need On Hand Qty", 0) or 0)
        available_qty = float(row.get("Need Available Qty", on_hand) or on_hand)

        value = 0
        if ready and unavailable:
            value += 120
        elif ready:
            value += 100
        elif unavailable:
            value += 80

        if blocked == "No matching source strain":
            value += 20
        elif blocked == "Source testing not passed":
            value += 10
        elif blocked == "New distillate opportunity":
            value += 30
        elif blocked in {"New flower opportunity", "New preroll opportunity"}:
            value += 30

        if available_qty <= 0:
            value += 10
        elif available_qty < 25:
            value += 5
        return value

    for row in production_rows:
        blocked = str(row.get("Blocked Reason", ""))
        ready = str(row.get("Scheduling Ready", "")) == "Yes"
        need_type = str(row.get("Need Type", ""))
        suggested_source_name = str(row.get("Suggested Source Name", "")).strip()

        if not ready and suggested_source_name == "":
            continue

        if ready:
            next_action = "Schedule packaging now"
            action_priority = "P1"
        elif blocked == "No matching source strain":
            next_action = "Create or pull matching source strain in bulk input"
            action_priority = "P1"
        elif blocked == "Source qty unavailable":
            next_action = "Increase matched source quantity"
            action_priority = "P2"
        elif blocked == "Source testing not passed":
            next_action = "Wait for test pass before scheduling"
            action_priority = "P2"
        else:
            next_action = "Review mapping and source data"
            action_priority = "P3"

        station = need_type if need_type in {"Cart", "Unit", "Preroll"} else "Other"
        matched_strain_norm = normalize_text(str(row.get("Matched Strain", "")))
        inventory_breakdown = format_packaged_breakdown(
            station,
            packaged_units_breakdown_by_station_strain.get((station, matched_strain_norm), {}),
        )

        todo_rows.append(
            {
                "Priority Rank": row.get("Priority Rank", ""),
                "Action Priority": action_priority,
                "Station": station,
                "Need SKU": row.get("Need SKU", ""),
                "Need Name": row.get("Need Name", ""),
                "Need Type": row.get("Need Type", ""),
                "Need List Qty": row.get("Need List Qty", row.get("Need On Hand Qty", "")),
                "Need Available Qty": row.get("Need Available Qty", row.get("Need On Hand Qty", "")),
                "Need Reserved Qty": row.get("Need Reserved Qty", ""),
                "Need On Hand Qty": row.get("Need On Hand Qty", ""),
                "Qty Basis": "units",
                "Potential 3.5g Units": "",
                "Potential 14g Units": "",
                "Potential Total Units": "",
                "Potential Unit Breakdown": "",
                "Unavailable": row.get("Unavailable", ""),
                "Scheduling Ready": row.get("Scheduling Ready", ""),
                "Blocked Reason": blocked,
                "Next Action": next_action,
                "Matched Strain": row.get("Matched Strain", ""),
                "Suggested Source Name": row.get("Suggested Source Name", ""),
                "Source Available Qty": row.get("Source Available Qty", ""),
                "Related Packaged Inventory": "; ".join(
                    packaged_inventory_by_station_strain.get((station, matched_strain_norm), [])
                )
                or "None",
                "Packaged Units In Inventory": packaged_units_total_by_station_strain.get(
                    (station, matched_strain_norm), 0
                ),
                "Packaged Unit Breakdown": inventory_breakdown,
            }
        )

    new_distillate_rows: List[dict] = []
    new_unit_rows: List[dict] = []
    new_preroll_rows: List[dict] = []
    for row in priority_rows:
        if str(row.get("Need Type", "")) != "Bulk":
            continue

        name = str(row.get("Name", ""))
        category = normalize_text(str(row.get("Category", "")))
        subcategory = normalize_text(str(row.get("Subcategory", "")))
        combined = normalize_text(f"{name} {category} {subcategory}")
        is_distillate = "distillate" in combined
        is_flower_bulk = "bulk flower" in combined or "bulk smalls" in combined or "small popcorn buds" in combined
        is_preroll_bulk = "preroll material" in combined or "trim" in combined

        avg_daily = float(row.get("Avg Daily Sales", 0) or 0)
        units_sold = float(row.get("Units Sold", 0) or 0)
        available_qty = float(row.get("Available Qty", 0) or 0)
        if avg_daily > 0 or units_sold > 0:
            continue
        if available_qty <= 0:
            continue

        strain_key = clean_strain_text(name)
        if not strain_key:
            strain_key = normalize_text(name)

        if is_distillate:
            generic_distillate_markers = {
                "bulk",
                "mixed",
                "raw mixed",
                "mixed distillate",
                "distillate bulk",
                "distillate finished product",
            }
            if strain_key not in generic_distillate_markers and "distillate bulk" not in combined and "mixed distillate" not in combined:
                related_inventory = "; ".join(packaged_inventory_by_station_strain.get(("Cart", strain_key), [])) or "None"
                new_distillate_rows.append(
                    {
                        "Priority Rank": row.get("Priority Rank", ""),
                        "Action Priority": "P1",
                        "Station": "Cart",
                        "Need SKU": row.get("SKU", ""),
                        "Need Name": row.get("Name", ""),
                        "Need Type": "Cart",
                        "Need List Qty": row.get("List Qty", available_qty),
                        "Need Available Qty": available_qty,
                        "Need Reserved Qty": row.get("Reserved Qty", 0),
                        "Need On Hand Qty": available_qty,
                        "Qty Basis": "grams",
                        "Potential 3.5g Units": "",
                        "Potential 14g Units": "",
                        "Potential Total Units": convert_grams_to_units(
                            available_qty,
                            "Cart",
                            subcategory=str(row.get("Subcategory", "")),
                            name=name,
                        ),
                        "Potential Unit Breakdown": f"1g Cart/AIO: {convert_grams_to_units(available_qty, 'Cart', subcategory=str(row.get('Subcategory', '')), name=name)}",
                        "Unavailable": row.get("Unavailable", "No"),
                        "Scheduling Ready": "No",
                        "Blocked Reason": "New distillate opportunity",
                        "Next Action": "Create new 1g Cart or 1g AIO Cart SKU from this distillate strain",
                        "Matched Strain": strain_key,
                        "Suggested Source Name": row.get("Name", ""),
                        "Source Available Qty": available_qty,
                        "Related Packaged Inventory": related_inventory,
                        "Packaged Units In Inventory": packaged_units_total_by_station_strain.get(("Cart", strain_key), 0),
                        "Packaged Unit Breakdown": format_packaged_breakdown(
                            "Cart",
                            packaged_units_breakdown_by_station_strain.get(("Cart", strain_key), {}),
                        ),
                    }
                )

        if is_flower_bulk:
            related_inventory = "; ".join(packaged_inventory_by_station_strain.get(("Unit", strain_key), [])) or "None"
            is_smalls_row = "bulk smalls" in combined or "small popcorn buds" in combined or "smalls" in normalize_text(name)
            potential_14g_units = int(math.floor(available_qty / 14.0))
            potential_unit_breakdown = (
                f"14g: {potential_14g_units}"
                if is_smalls_row
                else f"3.5g: {int(math.floor(available_qty / 3.5))} | 14g: {potential_14g_units}"
            )
            next_action_text = (
                "Create new 14g flower SKU from this smalls strain"
                if is_smalls_row
                else "Create new 3.5g/14g flower SKU from this bulk flower strain"
            )
            new_unit_rows.append(
                {
                    "Priority Rank": row.get("Priority Rank", ""),
                    "Action Priority": "P1",
                    "Station": "Unit",
                    "Need SKU": row.get("SKU", ""),
                    "Need Name": row.get("Name", ""),
                    "Need Type": "Unit",
                    "Need List Qty": row.get("List Qty", available_qty),
                    "Need Available Qty": available_qty,
                    "Need Reserved Qty": row.get("Reserved Qty", 0),
                    "Need On Hand Qty": available_qty,
                    "Qty Basis": "grams",
                    "Potential 3.5g Units": "",
                    "Potential 14g Units": "",
                    "Potential Total Units": convert_grams_to_units(
                        available_qty,
                        "Unit",
                        subcategory=str(row.get("Subcategory", "")),
                        name=name,
                    ),
                    "Potential Unit Breakdown": potential_unit_breakdown,
                    "Unavailable": row.get("Unavailable", "No"),
                    "Scheduling Ready": "No",
                    "Blocked Reason": "New flower opportunity",
                    "Next Action": next_action_text,
                    "Matched Strain": strain_key,
                    "Suggested Source Name": row.get("Name", ""),
                    "Source Available Qty": available_qty,
                    "Related Packaged Inventory": related_inventory,
                    "Packaged Units In Inventory": packaged_units_total_by_station_strain.get(("Unit", strain_key), 0),
                    "Packaged Unit Breakdown": format_packaged_breakdown(
                        "Unit",
                        packaged_units_breakdown_by_station_strain.get(("Unit", strain_key), {}),
                    ),
                }
            )

        if is_preroll_bulk:
            related_inventory = "; ".join(packaged_inventory_by_station_strain.get(("Preroll", strain_key), [])) or "None"
            new_preroll_rows.append(
                {
                    "Priority Rank": row.get("Priority Rank", ""),
                    "Action Priority": "P1",
                    "Station": "Preroll",
                    "Need SKU": row.get("SKU", ""),
                    "Need Name": row.get("Name", ""),
                    "Need Type": "Preroll",
                    "Need List Qty": row.get("List Qty", available_qty),
                    "Need Available Qty": available_qty,
                    "Need Reserved Qty": row.get("Reserved Qty", 0),
                    "Need On Hand Qty": available_qty,
                    "Qty Basis": "grams",
                    "Potential 3.5g Units": "",
                    "Potential 14g Units": "",
                    "Potential Total Units": convert_grams_to_units(
                        available_qty,
                        "Preroll",
                        subcategory=str(row.get("Subcategory", "")),
                        name=name,
                    ),
                    "Potential Unit Breakdown": f"1g Single: {int(math.floor(available_qty))} | 5pk (.7g): {int(math.floor(available_qty / 3.5))}",
                    "Unavailable": row.get("Unavailable", "No"),
                    "Scheduling Ready": "No",
                    "Blocked Reason": "New preroll opportunity",
                    "Next Action": "Create new pre-roll SKU from this preroll material strain",
                    "Matched Strain": strain_key,
                    "Suggested Source Name": row.get("Name", ""),
                    "Source Available Qty": available_qty,
                    "Related Packaged Inventory": related_inventory,
                    "Packaged Units In Inventory": packaged_units_total_by_station_strain.get(("Preroll", strain_key), 0),
                    "Packaged Unit Breakdown": format_packaged_breakdown(
                        "Preroll",
                        packaged_units_breakdown_by_station_strain.get(("Preroll", strain_key), {}),
                    ),
                }
            )

    todo_rows.extend(new_distillate_rows)
    todo_rows.extend(new_unit_rows)
    todo_rows.extend(new_preroll_rows)

    for row in todo_rows:
        station = str(row.get("Station", ""))
        breakdown = str(row.get("Packaged Unit Breakdown", ""))
        missing = find_missing_required_subtypes(station, breakdown)
        row["Missing Required Types"] = " | ".join(missing)
        row["Subtype Gap"] = "Yes" if missing else "No"

        gap_recommendation = packaging_recommendation_for_subtype_gap(
            station=station,
            need_name=str(row.get("Need Name", "")),
            breakdown=breakdown,
            missing_types=missing,
        )
        if gap_recommendation:
            row["Next Action"] = gap_recommendation
            continue

        available_qty = float(row.get("Need Available Qty", row.get("Need On Hand Qty", 0)) or 0)
        if available_qty <= 0:
            recommendation = packaging_recommendation_for_zero_inventory(
                station=station,
                need_name=str(row.get("Need Name", "")),
                breakdown=breakdown,
            )
            if recommendation:
                row["Next Action"] = recommendation

    todo_rows.sort(
        key=lambda r: (
            0 if str(r.get("Subtype Gap", "No")) == "Yes" else 1,
            -len([p for p in str(r.get("Missing Required Types", "")).split("|") if p.strip()]),
            {"P1": 0, "P2": 1, "P3": 2, "P4": 3}.get(str(r.get("Action Priority", "P4")), 4),
            -score(
                {
                    "Scheduling Ready": r.get("Scheduling Ready", ""),
                    "Unavailable": r.get("Unavailable", ""),
                    "Blocked Reason": r.get("Blocked Reason", ""),
                    "Need Available Qty": r.get("Need Available Qty", r.get("Need On Hand Qty", 0)),
                }
            ),
            float(r.get("Need Available Qty", r.get("Need On Hand Qty", 0)) or 0),
            str(r.get("Need SKU", "")),
        )
    )

    for idx, row in enumerate(todo_rows, start=1):
        row["ToDo Rank"] = idx

    return todo_rows


def load_config(path: Path) -> dict:
    if not path.exists():
        return DEFAULT_CONFIG
    with path.open("r", encoding="utf-8") as file:
        user_config = json.load(file)

    config = json.loads(json.dumps(DEFAULT_CONFIG))
    for key, value in user_config.items():
        if isinstance(value, dict) and isinstance(config.get(key), dict):
            config[key].update(value)
        else:
            config[key] = value
    return config


def main() -> None:
    parser = argparse.ArgumentParser(description="Build prioritized inventory reorder list from Distru exports.")
    parser.add_argument("--inventory", required=True, help="Path to Distru inventory valuation CSV")
    parser.add_argument(
        "--source-inventory",
        required=False,
        help="Optional inventory/packages CSV used only for source material availability (flower/smalls/trim/distillate)",
    )
    parser.add_argument("--sales", required=False, help="Optional path to Distru sales CSV (SKU + quantity)")
    parser.add_argument("--output", default="outputs/priority_list.csv", help="Output CSV path")
    parser.add_argument(
        "--production-output",
        default="outputs/production_plan.csv",
        help="Output CSV path for source-to-need production plan",
    )
    parser.add_argument(
        "--testing-status",
        required=False,
        help="Optional CSV with SKU and Testing Status; only TEST PASSED rows are schedulable",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=25,
        help="Maximum number of scheduling rows to output (default: 25)",
    )
    parser.add_argument(
        "--station-output-dir",
        default="outputs/stations",
        help="Directory for split station queues (cart/unit/preroll)",
    )
    parser.add_argument(
        "--todo-output",
        default="outputs/team_todo.csv",
        help="Output CSV path for compiled prioritized team to-do list",
    )
    parser.add_argument("--config", default="config/priority_rules.json", help="Optional config JSON path")
    args = parser.parse_args()

    config = load_config(Path(args.config))
    inventory_raw = parse_inventory(Path(args.inventory), config)
    inventory_cannabis = filter_inventory_for_priority(inventory_raw)
    priority_inventory = dict(inventory_cannabis)
    needs_inventory = filter_inventory_for_needs(inventory_cannabis)
    source_inventory_raw = parse_inventory(Path(args.source_inventory), config) if args.source_inventory else inventory_raw
    source_inventory_cannabis = filter_inventory_for_priority(source_inventory_raw)
    if args.source_inventory:
        for sku, item in source_inventory_cannabis.items():
            if sku not in priority_inventory and is_bulk_option(item):
                priority_inventory[sku] = item
    sales: Dict[str, SalesItem] = {}
    window_days = int(config["default_sales_window_days"])
    sales_mode = "inventory-only"
    if args.sales:
        try:
            sales, window_days = parse_sales(Path(args.sales), config)
            sales_mode = "inventory+sales"
        except Exception as exc:
            print(f"Sales file not usable ({exc}). Continuing with inventory-only prioritization.")

    testing_status = parse_testing_status(Path(args.testing_status)) if args.testing_status else {}

    prioritized = compute_priority_rows(priority_inventory, sales, window_days, config)
    if prioritized:
        write_csv(Path(args.output), prioritized)
    else:
        priority_path = Path(args.output)
        if priority_path.exists():
            priority_path.unlink()

    production_input_rows = [
        row for row in prioritized if str(row.get("Need Type", "")) in {"Cart", "Unit", "Preroll"}
    ]
    if not production_input_rows:
        fallback_rows: List[dict] = []
        for inv in needs_inventory.values():
            need_type = get_need_type(inv)
            if need_type == "":
                continue
            fallback_rows.append(
                {
                    "Priority Rank": 0,
                    "Priority": "Low",
                    "SKU": inv.sku,
                    "Name": inv.name,
                    "Available Qty": round(inv.available_qty, 2),
                    "Recommended Reorder Qty": 0,
                    "Unavailable": "Yes" if inv.available_qty <= 0 else "No",
                }
            )
        fallback_rows.sort(key=lambda r: (0 if r["Unavailable"] == "Yes" else 1, float(r["Available Qty"])))
        for i, row in enumerate(fallback_rows, start=1):
            row["Priority Rank"] = i
        production_input_rows = fallback_rows

    production = build_production_rows(
        production_input_rows,
        needs_inventory,
        source_inventory_cannabis,
        testing_status,
        args.top_n,
    )
    if production:
        write_csv(Path(args.production_output), production)
        write_station_outputs(Path(args.station_output_dir), production)
        todo_rows = build_todo_rows(production, prioritized, needs_inventory)
        if todo_rows:
            write_csv(Path(args.todo_output), todo_rows)
            write_station_todo_outputs(Path(args.station_output_dir), todo_rows)
            write_station_tile_views(Path(args.station_output_dir), todo_rows, top_n=10)
        else:
            todo_path = Path(args.todo_output)
            if todo_path.exists():
                todo_path.unlink()
            write_station_todo_outputs(Path(args.station_output_dir), [])
            tile_path = Path(args.station_output_dir) / "station_todo_tiles.html"
            if tile_path.exists():
                tile_path.unlink()
    else:
        output_path = Path(args.production_output)
        if output_path.exists():
            output_path.unlink()
        write_station_outputs(Path(args.station_output_dir), [])
        write_station_todo_outputs(Path(args.station_output_dir), [])
        tile_path = Path(args.station_output_dir) / "station_todo_tiles.html"
        if tile_path.exists():
            tile_path.unlink()
        todo_path = Path(args.todo_output)
        if todo_path.exists():
            todo_path.unlink()

    if prioritized:
        print(f"Created prioritized list: {args.output}")
    else:
        print("No actionable priority rows found.")
    if production:
        print(f"Created production plan: {args.production_output}")
        print(f"Created team todo list: {args.todo_output}")
        print(f"Created station queues in: {args.station_output_dir}")
    else:
        print("No actionable production rows found (requires valid source availability and TEST PASSED status).")
    print(
        f"Mode: {sales_mode} | Inventory items (raw): {len(inventory_raw)} | "
        f"Cannabis items used: {len(inventory_cannabis)} | "
        f"Priority items used: {len(priority_inventory)} | "
        f"Need items used: {len(needs_inventory)} | "
        f"Source items used: {len(source_inventory_cannabis)} | "
        f"SKUs with sales: {len(sales)} | Sales window days: {window_days}"
    )


if __name__ == "__main__":
    main()
