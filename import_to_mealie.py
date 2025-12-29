"""
Quick helper script to list Gousto recipe files and inspect their canonical URLs.
"""

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()


def get_required_env(var_name: str) -> str:
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value


def extract_canonical_slug(data: dict) -> str | None:
    canonical = (
        data.get("data", {})
        .get("entry", {})
        .get("seo", {})
        .get("canonical")
    )
    if not canonical:
        return None

    slug = canonical.rstrip("/").split("/")[-1]
    if not slug:
        return None
    return f"gousto-{slug}"


def fetch_recipe(session: requests.Session, base_url: str, slug: str) -> dict | None:
    url = f"{base_url}/recipes/{slug}"
    resp = session.get(url, timeout=10)
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 404:
        return None
    raise RuntimeError(f"{slug}: unexpected response {resp.status_code} from {url}")


def update_recipe(session: requests.Session, base_url: str, slug: str, recipe: dict, data: dict) -> None:
    gousto_title = (
        data.get("data", {})
        .get("entry", {})
        .get("title")
    )
    gousto_description = (
        data.get("data", {})
        .get("entry", {})
        .get("description")
    )
    payload = dict(recipe)  # send back existing recipe with updated name and description
    payload["name"] = gousto_title or recipe.get("name") or slug
    payload["description"] = gousto_description or recipe.get("description") or ""
    url = f"{base_url}/recipes/{slug}"
    resp = session.put(url, json=payload, timeout=10)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"{slug}: failed to update recipe ({resp.status_code}): {resp.text}")


def create_recipe(session: requests.Session, base_url: str, slug: str) -> None:
    url = f"{base_url}/recipes"
    payload = {"name": slug, "slug": slug}
    resp = session.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"{slug}: failed to create recipe ({resp.status_code}): {resp.text}")


def main() -> None:
    output_dir = Path(get_required_env("GOUSTO_OUTPUT_DIR"))
    if not output_dir.exists():
        raise RuntimeError(f"GOUSTO_OUTPUT_DIR does not exist: {output_dir}")

    mealie_base_url = get_required_env("MEALIE_BASE_URL").rstrip("/")
    mealie_token = get_required_env("MEALIE_TOKEN")

    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {mealie_token}"})

    errors: list[str] = []
    for path in sorted(output_dir.glob("*.json")):
        try:
            with path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as exc:
            errors.append(f"{path.name}: failed to read ({exc})")
            continue

        slug = extract_canonical_slug(data)
        if not slug:
            errors.append(f"{path.name}: canonical slug missing")
            continue

        try:
            existing = fetch_recipe(session, mealie_base_url, slug)
            if existing:
                update_recipe(session, mealie_base_url, slug, existing, data)
                print(f"{slug}: updated in Mealie")
                continue

            print(f"{slug}: not found in Mealie, creating...")
            create_recipe(session, mealie_base_url, slug)
            print(f"{slug}: created in Mealie")
            update_recipe(session, mealie_base_url, slug, existing, data)
            print(f"{slug}: updated in Mealie")

            exit(1)
        except Exception as exc:
            errors.append(f"{path.name}: {exc}")

    if errors:
        print("Errors encountered:")
        for err in errors:
            print(f" - {err}")
    else:
        print("No errors encountered.")


if __name__ == "__main__":
    main()
