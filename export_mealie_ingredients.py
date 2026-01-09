"""Export all Mealie foods (ingredients) to a text file."""

from __future__ import annotations

import argparse
import os
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


def normalize_list(payload: Any) -> list[dict[str, Any]]:
    """Normalize paginated Mealie responses into a list of dicts."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def normalize_text(value: Any) -> str | None:
    """Trim and collapse whitespace."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return " ".join(text.split())


def normalize_key(value: Any) -> str | None:
    """Lowercase and trim keys for loose matching."""
    text = normalize_text(value)
    return text.lower() if text else None


def extract_food_name(food: dict[str, Any]) -> tuple[str | None, str | None]:
    """Return (display_name, normalized_key) for a Mealie food record."""
    for key in ("name", "title", "label", "pluralName", "slug"):
        name = normalize_text(food.get(key))
        if name:
            return name, normalize_key(name)
    return None, None


def fetch_all_foods(session: requests.Session, base_url: str) -> list[dict[str, Any]]:
    """Fetch every food from Mealie, handling pagination."""
    foods: list[dict[str, Any]] = []
    url = f"{base_url}/foods"
    page = 1
    per_page = 100
    while True:
        resp = session.get(url, params={"page": page, "perPage": per_page}, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"foods: unexpected response {resp.status_code}: {resp.text}")
        page_items = normalize_list(resp.json())
        if not page_items:
            break
        foods.extend(page_items)
        if len(page_items) < per_page:
            break
        page += 1
    return foods


def build_lines(foods: list[dict[str, Any]], include_ids: bool, include_slugs: bool) -> list[str]:
    """Return sorted, de-duplicated ingredient lines for writing to a file."""
    seen_keys: set[str] = set()
    entries: list[tuple[str, str]] = []

    for food in foods:
        display_name, name_key = extract_food_name(food)
        slug = normalize_text(food.get("slug"))
        food_id = food.get("id")
        name = display_name or slug or (f"food-{food_id}" if food_id else "unknown food")
        dedupe_key = name_key or normalize_key(slug) or (str(food_id) if food_id else None)
        if dedupe_key and dedupe_key in seen_keys:
            continue
        if dedupe_key:
            seen_keys.add(dedupe_key)

        extras: list[str] = []
        if include_slugs and slug and slug != name:
            extras.append(f"slug: {slug}")
        if include_ids and food_id:
            extras.append(f"id: {food_id}")

        line = name if not extras else f"{name} ({', '.join(extras)})"
        entries.append((name.lower(), line))

    return [line for _, line in sorted(entries, key=lambda pair: pair[0])]


def main() -> None:
    parser = argparse.ArgumentParser(description="Export all Mealie foods (ingredients) to a text file.")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("mealie_ingredients.txt"),
        help="Where to write the ingredient list (default: mealie_ingredients.txt).",
    )
    parser.add_argument(
        "--include-ids",
        action="store_true",
        help="Include Mealie food IDs in the output for easier cross-referencing.",
    )
    parser.add_argument(
        "--include-slugs",
        action="store_true",
        help="Include slugs in the output when they differ from the display name.",
    )
    args = parser.parse_args()

    base_url = get_required_env("MEALIE_BASE_URL").rstrip("/")
    token = get_required_env("MEALIE_TOKEN")

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    print("Fetching ingredients from Mealie...")
    foods = fetch_all_foods(session, base_url)
    print(f"Found {len(foods)} foods.")

    lines = build_lines(foods, include_ids=args.include_ids, include_slugs=args.include_slugs)
    output_path = args.output
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {len(lines)} ingredient(s) to {output_path}")


if __name__ == "__main__":
    main()
