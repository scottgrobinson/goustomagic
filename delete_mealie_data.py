"""Delete all recipes and foods (ingredients) from a Mealie instance."""

from __future__ import annotations

import argparse
import os
from typing import Any, Iterable

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


def fetch_all(session: requests.Session, url: str, label: str) -> list[dict[str, Any]]:
    """Fetch all items from a paginated Mealie endpoint."""
    items: list[dict[str, Any]] = []
    page = 1
    per_page = 100
    while True:
        resp = session.get(url, params={"page": page, "perPage": per_page}, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"{label}: unexpected response {resp.status_code}: {resp.text}")
        page_items = normalize_list(resp.json())
        if not page_items:
            break
        items.extend(page_items)
        if len(page_items) < per_page:
            break
        page += 1
    return items


def delete_many(
    session: requests.Session,
    base_url: str,
    records: Iterable[dict[str, Any]],
    path_fn,
    label: str,
    dry_run: bool,
) -> None:
    """Delete resources by iterating over records and calling DELETE on each."""
    records = list(records)
    total = len(records)
    print(f"Deleting {total} {label}...")
    for idx, record in enumerate(records, start=1):
        target_path = path_fn(record)
        if not target_path:
            print(f"[{idx}/{total}] skipping {label[:-1]} with missing identifier: {record}")
            continue
        if dry_run:
            print(f"[{idx}/{total}] DRY RUN: would delete {target_path}")
            continue
        resp = session.delete(f"{base_url}{target_path}", timeout=15)
        if resp.status_code not in (200, 204):
            print(f"[{idx}/{total}] failed to delete {target_path} ({resp.status_code}): {resp.text}")
        else:
            print(f"[{idx}/{total}] deleted {target_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Delete all recipes and foods (ingredients) from Mealie.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip confirmation prompt.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be deleted without making changes.",
    )
    args = parser.parse_args()

    base_url = get_required_env("MEALIE_BASE_URL").rstrip("/")
    token = get_required_env("MEALIE_TOKEN")

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    recipes_endpoint = f"{base_url}/recipes"
    foods_endpoint = f"{base_url}/foods"

    print("Fetching recipes...")
    recipes = fetch_all(session, recipes_endpoint, "recipes")
    print(f"Found {len(recipes)} recipes.")

    print("Fetching foods (ingredients)...")
    foods = fetch_all(session, foods_endpoint, "foods")
    print(f"Found {len(foods)} foods.")

    if not args.force and not args.dry_run:
        response = input("Type 'delete' to remove all recipes and foods: ").strip().lower()
        if response != "delete":
            print("Aborted.")
            return

    # Delete recipes first so foods are no longer referenced.
    delete_many(
        session,
        base_url=base_url,
        records=recipes,
        path_fn=lambda recipe: f"/recipes/{recipe.get('slug') or recipe.get('id')}",
        label="recipes",
        dry_run=args.dry_run,
    )

    delete_many(
        session,
        base_url=base_url,
        records=foods,
        path_fn=lambda food: f"/foods/{food.get('id')}",
        label="foods",
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
