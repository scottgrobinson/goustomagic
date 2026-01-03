"""Verify that Mealie recipes match a static list of expected ingredients and quantities."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()


def get_required_env(var_name: str) -> str:
    """Return required environment variable or raise if missing."""
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value


def _normalize_key(value: str | None) -> str | None:
    """Lowercase and trim keys for loose matching."""
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _normalize_recipe_slug(value: str) -> str:
    """Normalize recipe identifier by stripping .json and lowercasing."""
    text = str(value).strip()
    if text.endswith(".json"):
        text = text[:-5]
    return text.strip().lower()


def _parse_quantity(raw: Any) -> float | None:
    """Convert a numeric-like value to float, returning None on failure."""
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _extract_unit_name(unit_obj: Any) -> tuple[str | None, str | None]:
    """Return (display, key) for a unit value that may be a dict or string."""
    if unit_obj is None:
        return None, None
    if isinstance(unit_obj, str):
        name = unit_obj.strip()
        return (name or None), _normalize_key(name)
    if isinstance(unit_obj, dict):
        for key in ("name", "abbreviation", "pluralName", "pluralAbbreviation"):
            val = unit_obj.get(key)
            if val:
                name = str(val).strip()
                return name or None, _normalize_key(name)
    return None, None


def _extract_food_name(ingredient: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return (display, key) for the ingredient food name."""
    food = ingredient.get("food")
    if isinstance(food, str):
        name = food.strip()
        if name:
            return name, _normalize_key(name)
    if isinstance(food, dict):
        for key in ("name", "title", "label", "pluralName", "slug"):
            val = food.get(key)
            if val:
                name = str(val).strip()
                if name:
                    return name, _normalize_key(name)
    for key in ("foodName", "name", "text", "display", "originalText"):
        val = ingredient.get(key)
        if val:
            name = str(val).strip()
            if name:
                return name, _normalize_key(name)
    return None, None


def _canonical_expected_entry(raw: Any) -> dict[str, Any]:
    """Normalize an expected ingredient entry."""
    if not isinstance(raw, dict):
        raise ValueError(f"Expected ingredient entry to be an object, got: {raw!r}")
    name = raw.get("name") or raw.get("food") or raw.get("ingredient")
    if not name:
        raise ValueError(f"Expected ingredient entry to include a name: {raw!r}")
    quantity = _parse_quantity(raw.get("quantity"))
    unit_display = raw.get("unit")
    unit_key = _normalize_key(unit_display) if unit_display else None
    return {
        "name": str(name).strip(),
        "key": _normalize_key(name),
        "quantity": quantity,
        "unit_display": unit_display,
        "unit_key": unit_key,
    }


def load_expected(path: Path) -> dict[str, list[dict[str, Any]]]:
    """Load expected ingredient lists keyed by recipe slug."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    expected: dict[str, list[dict[str, Any]]] = {}

    def add_recipe(slug: str, entries: Any) -> None:
        normalized_slug = _normalize_recipe_slug(slug)
        if not normalized_slug:
            raise ValueError(f"Recipe slug missing or empty: {slug!r}")
        if not isinstance(entries, list):
            raise ValueError(f"Expected ingredient list for {slug}, got {type(entries).__name__}")
        expected[normalized_slug] = [_canonical_expected_entry(entry) for entry in entries]

    if isinstance(data, dict) and isinstance(data.get("recipes"), list):
        for recipe in data["recipes"]:
            slug = recipe.get("slug") or recipe.get("id") or recipe.get("name")
            add_recipe(slug, recipe.get("ingredients") or [])
    elif isinstance(data, dict):
        for slug, entries in data.items():
            add_recipe(slug, entries)
    else:
        raise ValueError("Expected JSON object mapping recipe slugs to ingredient lists.")

    return expected


def load_recipe_list_file(path: Path) -> list[str]:
    """Read recipe identifiers (one per line) from a text file."""
    recipes: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            recipes.append(_normalize_recipe_slug(text))
    return recipes


def fetch_recipe(session: requests.Session, base_url: str, slug: str) -> dict[str, Any] | None:
    """Fetch a recipe from Mealie by slug."""
    url = f"{base_url}/recipes/{slug}"
    resp = session.get(url, timeout=15)
    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        raise RuntimeError(f"{slug}: unexpected response {resp.status_code}: {resp.text}")
    return resp.json()


def collect_actual_ingredients(recipe: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize Mealie recipe ingredients for comparison."""
    normalized: list[dict[str, Any]] = []
    for ing in recipe.get("recipeIngredient") or []:
        name_display, name_key = _extract_food_name(ing)
        unit_display, unit_key = _extract_unit_name(ing.get("unit"))
        quantity = _parse_quantity(ing.get("quantity"))
        normalized.append(
            {
                "name": name_display,
                "key": name_key,
                "quantity": quantity,
                "unit_display": unit_display,
                "unit_key": unit_key,
                "raw": ing,
            }
        )
    return normalized


def compare_ingredients(
    expected_entries: list[dict[str, Any]],
    actual_entries: list[dict[str, Any]],
    tolerance: float,
    allow_missing_expected: bool,
) -> dict[str, Any]:
    """Compare expected vs actual ingredient lists."""
    IGNORED_INGREDIENT_KEYS = {
        _normalize_key("Vegetable oil"),
        _normalize_key("Olive oil"),
        _normalize_key("Pepper"),
        _normalize_key("Salt"),
        _normalize_key("Butter"),
        _normalize_key("Flour"),
        _normalize_key("Milk"),
        _normalize_key("Sugar"),
    }

    expected_keys = {entry["key"] for entry in expected_entries if entry.get("key")}
    actual_map: dict[str, dict[str, Any]] = {}
    extras_unmatched: list[dict[str, Any]] = []
    for ing in actual_entries:
        key = ing.get("key")
        if key in IGNORED_INGREDIENT_KEYS and key not in expected_keys:
            continue
        if key and key not in actual_map:
            actual_map[ing["key"]] = ing
        else:
            extras_unmatched.append(ing)

    missing: list[str] = []
    missing_expected: list[str] = []
    quantity_mismatches: list[str] = []
    unit_mismatches: list[str] = []

    for exp in expected_entries:
        actual = actual_map.pop(exp["key"], None)
        actual_qty = actual.get("quantity") if actual else None
        actual_unit_key = actual.get("unit_key") if actual else None
        actual_unit_display = actual.get("unit_display") if actual else None

        # Only flag missing expected data when the recipe actually has that value to compare.
        if exp.get("quantity") is None and actual_qty is not None:
            missing_expected.append(f"{exp['name']}: expected quantity not provided")
        if exp.get("unit_display") is None and actual_unit_key is not None:
            missing_expected.append(f"{exp['name']}: expected unit not provided")

        if not actual:
            missing.append(exp["name"])
            continue

        if exp["quantity"] is not None:
            if actual_qty is None or abs(exp["quantity"] - actual_qty) > tolerance:
                quantity_mismatches.append(
                    f"{exp['name']}: expected {exp['quantity']}, found {actual_qty}"
                )

        if exp["unit_key"]:
            if not actual_unit_key:
                unit_mismatches.append(f"{exp['name']}: expected unit '{exp['unit_display']}', found none")
            elif actual_unit_key != exp["unit_key"]:
                unit_mismatches.append(
                    f"{exp['name']}: expected unit '{exp['unit_display']}', found '{actual_unit_display}'"
                )

    extras = [ing["name"] or ing["raw"] for ing in actual_map.values()]
    extras.extend(ing["name"] or ing["raw"] for ing in extras_unmatched if ing.get("key") not in IGNORED_INGREDIENT_KEYS)

    ok = not (
        missing
        or quantity_mismatches
        or unit_mismatches
        or extras
        or (missing_expected and not allow_missing_expected)
    )
    return {
        "ok": ok,
        "missing": missing,
        "quantity_mismatches": quantity_mismatches,
        "unit_mismatches": unit_mismatches,
        "extra": extras,
        "missing_expected": missing_expected,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Check that Mealie recipes match a static ingredient list."
    )
    parser.add_argument(
        "--expected",
        type=Path,
        default=Path("expected_ingredients.json"),
        help="Path to JSON file containing expected ingredients.",
    )
    parser.add_argument(
        "--recipe",
        action="append",
        dest="recipes",
        help="Recipe slug to check (can be provided multiple times).",
    )
    parser.add_argument(
        "--recipes-file",
        type=Path,
        help="Optional file with one recipe slug per line (supports .json names).",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.01,
        help="Allowed numeric delta when comparing quantities.",
    )
    parser.add_argument(
        "--allow-missing-expected",
        action="store_true",
        help="Do not fail when expected quantity or unit is missing (will still report).",
    )
    args = parser.parse_args()

    base_url = get_required_env("MEALIE_BASE_URL").rstrip("/")
    token = get_required_env("MEALIE_TOKEN")

    expected_path = args.expected
    if not expected_path.exists():
        raise FileNotFoundError(f"Expected data file not found: {expected_path}")
    expected = load_expected(expected_path)

    recipe_ids: list[str] = []
    if args.recipes_file:
        recipe_ids.extend(load_recipe_list_file(args.recipes_file))
    if args.recipes:
        recipe_ids.extend([_normalize_recipe_slug(r) for r in args.recipes])
    if not recipe_ids:
        recipe_ids.extend(expected.keys())

    # Deduplicate while preserving order.
    seen: set[str] = set()
    normalized_recipes: list[str] = []
    for slug in recipe_ids:
        if slug in seen or not slug:
            continue
        seen.add(slug)
        normalized_recipes.append(slug)

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    failures = 0
    for slug in normalized_recipes:
        expected_entries = expected.get(slug)
        if expected_entries is None:
            print(f"{slug}: no expected ingredients found; skipping")
            failures += 1
            continue

        recipe = fetch_recipe(session, base_url, slug)
        if not recipe:
            print(f"{slug}: not found in Mealie")
            failures += 1
            continue

        actual_entries = collect_actual_ingredients(recipe)
        result = compare_ingredients(
            expected_entries,
            actual_entries,
            tolerance=args.tolerance,
            allow_missing_expected=args.allow_missing_expected,
        )

        if result["ok"]:
            print(f"{slug}: OK ({len(expected_entries)} ingredients)")
            continue

        failures += 1
        print(f"{slug}: mismatches found")
        if result.get("missing_expected"):
            for line in result["missing_expected"]:
                print(f"  expected-data: {line}")
        if result["missing"]:
            print(f"  missing: {', '.join(result['missing'])}")
        if result["quantity_mismatches"]:
            for line in result["quantity_mismatches"]:
                print(f"  quantity: {line}")
        if result["unit_mismatches"]:
            for line in result["unit_mismatches"]:
                print(f"  unit: {line}")
        if result["extra"]:
            extras = ", ".join(str(item) for item in result["extra"])
            print(f"  extra in Mealie: {extras}")

    if failures:
        print(f"Completed with {failures} recipe(s) failing ingredient checks.")
        sys.exit(1)
    print("All recipes match expected ingredients.")


if __name__ == "__main__":
    main()
