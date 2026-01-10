"""Import Gousto recipe JSON files into Mealie, creating categories and recipes as needed."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import mimetypes
import os
import re
import threading
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

from slugify import slugify

Category = dict[str, Any]
Recipe = dict[str, Any]
Food = dict[str, Any]
Unit = dict[str, Any]
Tag = dict[str, Any]

NUTRITION_KEYS = (
    "calories",
    "carbohydrateContent",
    "cholesterolContent",
    "fatContent",
    "fiberContent",
    "proteinContent",
    "saturatedFatContent",
    "sodiumContent",
    "sugarContent",
    "transFatContent",
    "unsaturatedFatContent",
)

DEFAULT_RECIPE_SETTINGS = {
    "public": True,
    "showNutrition": True,
    "showAssets": False,
    "landscapeView": False,
    "disableComments": False,
    "locked": False,
}

# Cache of Mealie categories by normalized key (name or slug)
CATEGORIES_BY_KEY: dict[str, Category] = {}
FOODS_BY_KEY: dict[str, Food] = {}
UNITS_BY_KEY: dict[str, Unit] = {}
TAGS_BY_KEY: dict[str, Tag] = {}
warnings: list[str] = []
errors: list[str] = []
INGREDIENT_MAP: dict[str, str] = {}
INGREDIENT_MAP_BY_KEY: dict[str, str] = {}
CACHE_LOCK = threading.RLock()
WARNINGS_LOCK = threading.Lock()

load_dotenv()


def append_warning(message: str) -> None:
    """Append a warning in a threadsafe way."""
    with WARNINGS_LOCK:
        warnings.append(message)


def get_required_env(var_name: str) -> str:
    """Return required environment variable or raise if missing."""
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value


def fetch_recipe(session: requests.Session, base_url: str, slug: str) -> dict | None:
    """Fetch a recipe by slug from Mealie."""
    url = f"{base_url}/recipes/{slug}"
    resp = session.get(url, timeout=10)
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 404:
        return None
    raise RuntimeError(f"{slug}: unexpected response {resp.status_code} from {url}")


def update_recipe(
    session: requests.Session,
    base_url: str,
    slug: str,
    recipe: Recipe,
    entry: dict[str, Any],
    recipe_categories: list[Category] | None,
    recipe_instructions: list[dict[str, Any]] | None,
    recipe_ingredients: list[dict[str, Any]] | None,
    recipe_tags: list[dict[str, Any]] | None,
) -> None:
    """Update recipe fields in Mealie with Gousto data, nutrition, settings, ingredients, and categories."""
    if recipe is None:
        raise ValueError("existing recipe payload is missing")
    gousto_title = entry.get("title")
    gousto_description = entry.get("description")
    prep_minutes = entry.get("prep_times", {}).get("for_2")
    payload = dict(recipe)  # send back existing recipe with updated name and description
    payload["name"] = gousto_title or recipe.get("name") or slug
    payload["description"] = gousto_description or recipe.get("description") or ""
    payload["recipeServings"] = 2
    if prep_minutes is not None:
        payload["totalTime"] = f"{prep_minutes}m"
    nutrition = build_nutrition(entry, recipe.get("nutrition"))
    if nutrition:
        payload["nutrition"] = nutrition
    settings = dict(recipe.get("settings") or {})
    settings.update(DEFAULT_RECIPE_SETTINGS)
    payload["settings"] = settings
    payload["recipeCategory"] = recipe_categories if recipe_categories is not None else (recipe.get("recipeCategory") or [])
    if recipe_instructions is not None:
        payload["recipeInstructions"] = recipe_instructions
    if recipe_ingredients is not None:
        payload["recipeIngredient"] = recipe_ingredients
    if recipe_tags is not None:
        payload["tags"] = recipe_tags
    url = f"{base_url}/recipes/{slug}"
    resp = session.put(url, json=payload, timeout=10)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"{slug}: failed to update recipe ({resp.status_code}): {resp.text}")


def create_recipe(session: requests.Session, base_url: str, slug: str) -> None:
    """Create a minimal recipe placeholder in Mealie so it can be updated."""
    url = f"{base_url}/recipes"
    payload = {"name": slug, "slug": slug}
    resp = session.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"{slug}: failed to create recipe ({resp.status_code}): {resp.text}")


def _category_name(cat: Category) -> str | None:
    """Extract the preferred category name."""
    return cat.get("name") or cat.get("title")


def _category_key(name: str | None) -> str | None:
    """Normalize category identifiers for consistent matching."""
    if not name:
        return None
    return name.strip().lower()


def _food_key(name: str | None) -> str | None:
    """Normalize food identifiers for consistent matching."""
    return _category_key(name)


def _unit_key(name: str | None) -> str | None:
    """Normalize unit identifiers for consistent matching."""
    return _category_key(name)


def _tag_key(name: str | None) -> str | None:
    """Normalize tag identifiers for consistent matching."""
    return _category_key(name)


def load_ingredient_map(map_path: Path) -> None:
    """Load ingredient name mappings from a JSON file."""
    INGREDIENT_MAP.clear()
    INGREDIENT_MAP_BY_KEY.clear()
    if not map_path.exists():
        append_warning(f"Ingredient map not found: {map_path}")
        return
    try:
        with map_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        raise RuntimeError(f"Ingredient map failed to load ({map_path}): {exc}") from exc
    if not isinstance(data, dict):
        raise RuntimeError(f"Ingredient map {map_path} is not a JSON object")
    for raw_key, raw_value in data.items():
        key_text = str(raw_key).strip() if raw_key is not None else ""
        value_text = str(raw_value).strip() if raw_value is not None else ""
        if not key_text or not value_text:
            continue
        INGREDIENT_MAP[key_text] = value_text
        normalized_key = _food_key(key_text)
        if normalized_key:
            INGREDIENT_MAP_BY_KEY.setdefault(normalized_key, value_text)


def map_ingredient_name(name: str | None) -> str | None:
    """Return the mapped ingredient name if present."""
    if not name:
        return None
    mapped = INGREDIENT_MAP.get(name)
    if mapped:
        return mapped
    key = _food_key(name)
    if key and key in INGREDIENT_MAP_BY_KEY:
        return INGREDIENT_MAP_BY_KEY[key]
    return name


def _normalize_category_list(payload: Any) -> list[dict[str, Any]]:
    """Coerce Mealie category responses into a list of category dicts."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("items", "data", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def _normalize_food_list(payload: Any) -> list[dict[str, Any]]:
    """Coerce Mealie food responses into a list of food dicts."""
    return _normalize_category_list(payload)


def _normalize_unit_list(payload: Any) -> list[dict[str, Any]]:
    """Coerce Mealie unit responses into a list of unit dicts."""
    return _normalize_category_list(payload)


def _normalize_tag_list(payload: Any) -> list[dict[str, Any]]:
    """Coerce Mealie tag responses into a list of tag dicts."""
    return _normalize_category_list(payload)


def load_existing_categories(session: requests.Session, base_url: str) -> None:
    """Preload category cache from Mealie so we can re-use existing records."""
    categories_by_key: dict[str, Category] = {}
    seen_ids: set[str] = set()
    url = f"{base_url}/organizers/categories"
    page = 1
    per_page = 100
    while True:
        resp = session.get(url, params={"page": page, "perPage": per_page}, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"categories: unexpected response {resp.status_code}: {resp.text}")

        categories = _normalize_category_list(resp.json())
        if not categories:
            break

        for cat in categories:
            cat_id = cat.get("id")
            if cat_id and cat_id in seen_ids:
                continue
            if cat_id:
                seen_ids.add(cat_id)
            for key in (_category_key(_category_name(cat)), _category_key(cat.get("slug"))):
                if key:
                    categories_by_key[key] = cat  # cache by name/slug for quick lookups

        if len(categories) < per_page:
            break
        page += 1
    with CACHE_LOCK:
        CATEGORIES_BY_KEY.clear()
        CATEGORIES_BY_KEY.update(categories_by_key)


def load_existing_foods(session: requests.Session, base_url: str) -> None:
    """Preload food cache from Mealie so we can re-use existing ingredient records."""
    foods_by_key: dict[str, Food] = {}
    seen_ids: set[str] = set()
    url = f"{base_url}/foods"
    page = 1
    per_page = 100
    while True:
        resp = session.get(url, params={"page": page, "perPage": per_page}, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"foods: unexpected response {resp.status_code}: {resp.text}")

        foods = _normalize_food_list(resp.json())
        if not foods:
            break

        for food in foods:
            food_id = food.get("id")
            if food_id and food_id in seen_ids:
                continue
            if food_id:
                seen_ids.add(food_id)
            for key in (_food_key(food.get("name")), _food_key(food.get("slug"))):
                if key:
                    foods_by_key[key] = food  # cache by name/slug

        if len(foods) < per_page:
            break
        page += 1
    with CACHE_LOCK:
        FOODS_BY_KEY.clear()
        FOODS_BY_KEY.update(foods_by_key)

def load_existing_units(session: requests.Session, base_url: str) -> None:
    """Preload unit cache from Mealie for unit lookups."""
    units_by_key: dict[str, Unit] = {}
    seen_ids: set[str] = set()
    url = f"{base_url}/units"
    page = 1
    per_page = 100
    while True:
        resp = session.get(url, params={"page": page, "perPage": per_page}, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"units: unexpected response {resp.status_code}: {resp.text}")

        units = _normalize_unit_list(resp.json())
        if not units:
            break

        for unit in units:
            unit_id = unit.get("id")
            if unit_id and unit_id in seen_ids:
                continue
            if unit_id:
                seen_ids.add(unit_id)
            for key in (
                _unit_key(unit.get("name")),
                _unit_key(unit.get("pluralName")),
                _unit_key(unit.get("abbreviation")),
                _unit_key(unit.get("pluralAbbreviation")),
            ):
                if key:
                    units_by_key[key] = unit

        if len(units) < per_page:
            break
        page += 1
    with CACHE_LOCK:
        UNITS_BY_KEY.clear()
        UNITS_BY_KEY.update(units_by_key)


def load_existing_tags(session: requests.Session, base_url: str) -> None:
    """Preload tag cache from Mealie so we can re-use existing tags."""
    tags_by_key: dict[str, Tag] = {}
    seen_ids: set[str] = set()
    url = f"{base_url}/organizers/tags"
    page = 1
    per_page = 100
    while True:
        resp = session.get(url, params={"page": page, "perPage": per_page}, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(f"tags: unexpected response {resp.status_code}: {resp.text}")

        tags = _normalize_tag_list(resp.json())
        if not tags:
            break

        for tag in tags:
            tag_id = tag.get("id")
            if tag_id and tag_id in seen_ids:
                continue
            if tag_id:
                seen_ids.add(tag_id)
            for key in (_tag_key(tag.get("name")), _tag_key(tag.get("slug"))):
                if key:
                    tags_by_key[key] = tag

        if len(tags) < per_page:
            break
        page += 1
    with CACHE_LOCK:
        TAGS_BY_KEY.clear()
        TAGS_BY_KEY.update(tags_by_key)


def ensure_food(session: requests.Session, base_url: str, name: str, description: str | None = None) -> dict[str, Any]:
    """Return a food object, creating it in Mealie if needed."""
    name_key = _food_key(name)
    with CACHE_LOCK:
        cached = FOODS_BY_KEY.get(name_key) if name_key else None
    if cached:
        return cached

    url = f"{base_url}/foods"
    payload: dict[str, Any] = {"name": name, "pluralName": name}
    resp = session.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 201):
        # If creation fails because it already exists, refresh the cache and retry lookup.
        if resp.status_code in (400, 409):
            load_existing_foods(session, base_url)
            with CACHE_LOCK:
                cached = FOODS_BY_KEY.get(name_key) if name_key else None
            if cached:
                return cached
        raise RuntimeError(f"food '{name}': failed to create ({resp.status_code}): {resp.text}")

    food = resp.json()
    with CACHE_LOCK:
        for key in (_food_key(food.get("name")), _food_key(food.get("slug"))):
            if key:
                FOODS_BY_KEY[key] = food
    return food


def ensure_category(session: requests.Session, base_url: str, title: str) -> dict[str, Any]:
    """Return a category object, creating it in Mealie if needed."""
    name_key = _category_key(title)
    slug_value = slugify(title)
    slug_key = _category_key(slug_value)
    with CACHE_LOCK:
        for key in (name_key, slug_key):
            cached = CATEGORIES_BY_KEY.get(key) if key else None
            if cached:
                return cached

    url = f"{base_url}/organizers/categories"
    payload = {"name": title, "slug": slug_value}
    resp = session.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 201):
        # If creation fails because it already exists, refresh the cache and retry lookup.
        if resp.status_code in (400, 409):
            load_existing_categories(session, base_url)
            with CACHE_LOCK:
                for key in (name_key, slug_key):
                    cached = CATEGORIES_BY_KEY.get(key) if key else None
                    if cached:
                        return cached
        raise RuntimeError(f"category '{title}': failed to create ({resp.status_code}): {resp.text}")

    category = resp.json()
    name = _category_name(category) or title
    with CACHE_LOCK:
        for key in (_category_key(name), _category_key(category.get("slug"))):
            if key:
                CATEGORIES_BY_KEY[key] = category
    return category


def ensure_tag(session: requests.Session, base_url: str, name: str) -> dict[str, Any]:
    """Return a tag object, creating it in Mealie if needed."""
    name_key = _tag_key(name)
    slug_value = slugify(name)
    slug_key = _tag_key(slug_value)
    with CACHE_LOCK:
        for key in (name_key, slug_key):
            cached = TAGS_BY_KEY.get(key) if key else None
            if cached:
                return cached

    url = f"{base_url}/organizers/tags"
    payload = {"name": name, "slug": slug_value}
    resp = session.post(url, json=payload, timeout=10)
    if resp.status_code not in (200, 201):
        if resp.status_code in (400, 409):
            load_existing_tags(session, base_url)
            with CACHE_LOCK:
                for key in (name_key, slug_key):
                    cached = TAGS_BY_KEY.get(key) if key else None
                    if cached:
                        return cached
        raise RuntimeError(f"tag '{name}': failed to create ({resp.status_code}): {resp.text}")

    tag = resp.json()
    with CACHE_LOCK:
        for key in (_tag_key(tag.get("name")), _tag_key(tag.get("slug"))):
            if key:
                TAGS_BY_KEY[key] = tag
    return tag


def gather_category_titles(entry: dict[str, Any]) -> list[str]:
    """Extract unique category titles (including cuisine) from a Gousto entry."""
    titles: list[str] = []
    for cat in entry.get("categories") or []:
        title = cat.get("title")
        if title:
            titles.append(title)
    cuisine = entry.get("cuisine")
    if isinstance(cuisine, dict):
        cuisine_title = cuisine.get("title")
        if cuisine_title:
            titles.append(cuisine_title)

    unique_titles: list[str] = []
    seen_category_keys: set[str] = set()
    for title in titles:
        key = _category_key(title)
        if not key or key in seen_category_keys:
            continue
        seen_category_keys.add(key)
        unique_titles.append(title)
    return unique_titles


def gather_ingredients(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Collect ingredients and basics as a single list."""
    ingredients: list[dict[str, Any]] = []
    ingredients.extend(entry.get("ingredients") or [])
    ingredients.extend(entry.get("basics") or [])
    return ingredients


def ingredient_label_and_name(item: dict[str, Any]) -> tuple[str, str | None]:
    """Return (label, cleaned food name) for an ingredient/basics entry."""
    raw_name = item.get("name") or item.get("title") or item.get("label")
    label = item.get("label") or raw_name or ""
    name = map_ingredient_name(_clean_food_name(raw_name, label))
    return label, name


def _ingredient_code(ingredient: dict[str, Any]) -> str | None:
    """Extract a code (e.g., I-xx-xxx) from an ingredient for SKU matching."""
    if ingredient.get("code"):
        return str(ingredient["code"]).strip().lower()
    title = ingredient.get("title") or ""
    match = re.search(r"(I-[A-Za-z0-9-]+)$", title)
    if match:
        return match.group(1).lower()
    return None


def select_portion_skus(entry: dict[str, Any], portions: int = 2) -> list[dict[str, Any]] | None:
    """Return ingredient_skus for the requested portions, if available."""
    portion_sizes = entry.get("portion_sizes") or []
    if not portion_sizes:
        return None
    for ps in portion_sizes:
        if ps.get("portions") == portions and ps.get("is_offered"):
            return ps.get("ingredients_skus") or []
    for ps in portion_sizes:
        if ps.get("portions") == portions:
            return ps.get("ingredients_skus") or []
    return None


def gather_allergen_tags(entry: dict[str, Any]) -> list[str]:
    """Build allergen tag names from the entry."""
    tags: list[str] = []
    for allergen in entry.get("allergens") or []:
        title = allergen.get("title") or allergen.get("slug")
        if not title:
            continue
        normalized = slugify(title.strip())
        tags.append(f"allergen:{normalized}")
    # Deduplicate while preserving order
    seen: set[str] = set()
    unique: list[str] = []
    for name in tags:
        if name in seen:
            continue
        seen.add(name)
        unique.append(name)
    return unique


def _mg_to_grams(value: Any) -> float | None:
    """Convert milligrams to grams."""
    if value is None:
        return None
    try:
        return float(value) / 1000.0
    except (TypeError, ValueError):
        return None


def _salt_mg_to_sodium_mg(value: Any) -> float | None:
    """Convert salt (NaCl) milligrams to sodium milligrams using 2.5:1 salt-to-sodium ratio."""
    if value is None:
        return None
    try:
        return float(value) / 2.5
    except (TypeError, ValueError):
        return None


def build_nutrition(entry: dict[str, Any], existing: dict[str, Any] | None) -> dict[str, Any] | None:
    """Merge Gousto per-portion nutrition into existing Mealie nutrition fields."""
    nutrition_block = (entry.get("nutrional_infromation") or entry.get("nutritional_information") or {}).get("per_portion") or {}
    nutrition: dict[str, Any] = dict(existing or {})
    updates = {
        "calories": nutrition_block.get("energy_kcal"),
        "carbohydrateContent": _mg_to_grams(nutrition_block.get("carbs_mg")),
        "cholesterolContent": None,  # Source data does not supply cholesterol.
        "fatContent": _mg_to_grams(nutrition_block.get("fat_mg")),
        "fiberContent": _mg_to_grams(nutrition_block.get("fibre_mg")),
        "proteinContent": _mg_to_grams(nutrition_block.get("protein_mg")),
        "saturatedFatContent": _mg_to_grams(nutrition_block.get("fat_saturates_mg")),
        "sodiumContent": _salt_mg_to_sodium_mg(nutrition_block.get("salt_mg")),
        "sugarContent": _mg_to_grams(nutrition_block.get("carbs_sugars_mg")),
        "transFatContent": None,
        "unsaturatedFatContent": None,
    }

    changed = False
    for key, value in updates.items():
        if value is not None:
            nutrition[key] = value
            changed = True

    for key in NUTRITION_KEYS:
        nutrition.setdefault(key, None)

    if not changed and not existing:
        return None
    return nutrition


def extract_entry(data: dict[str, Any]) -> dict[str, Any]:
    """Pull the nested entry block from a Gousto JSON payload."""
    return data.get("data", {}).get("entry", {})


def html_to_text(html: str) -> str:
    """Convert basic HTML to plaintext with paragraph spacing."""
    if not html:
        return ""
    text = html.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    text = re.sub(r"</p\s*>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return text.strip()


def build_instructions(
    ordered_steps: list[dict[str, Any]],
    recipe_id: str | None,
    step_asset_map: dict[int, str],
) -> list[dict[str, Any]]:
    """Build Mealie recipeInstructions from ordered Gousto cooking_instructions, appending HTML images."""
    instructions: list[dict[str, Any]] = []
    for idx, step in enumerate(ordered_steps):
        text = html_to_text(step.get("instruction") or "").replace("\n", "<br/>")
        if recipe_id and idx in step_asset_map:
            filename = step_asset_map[idx]
            image_src = f"/api/media/recipes/{recipe_id}/assets/{filename}"
            text = (
                f"{text}<br/><br/>"
                f'<img src="{image_src}" alt="Step image" width="100%" height="100%" />'
            )
        instructions.append({"text": text, "ingredientReferences": []})
    return instructions


def build_recipe_ingredients(
    session: requests.Session,
    base_url: str,
    entry: dict[str, Any],
    portion_skus: list[dict[str, Any]] | None = None,
    warn_prefix: str | None = None,
) -> list[dict[str, Any]]:
    """Build recipeIngredient payload, ensuring foods exist, attaching known units, and merging duplicate foods."""
    all_items = gather_ingredients(entry)
    code_to_item: dict[str, dict[str, Any]] = {}
    for itm in all_items:
        code = _ingredient_code(itm)
        if code and code not in code_to_item:
            code_to_item[code] = itm
        # Index by gousto_uuid as well for fallback matching
        gousto_id = itm.get("gousto_uuid") or itm.get("uid")
        if gousto_id:
            code_to_item[str(gousto_id).strip().lower()] = itm

    recipe_ingredients: list[dict[str, Any]] = []
    ingredients_by_key: dict[str, list[dict[str, Any]]] = {}

    if portion_skus:
        ordered_items: list[tuple[dict[str, Any], dict[str, Any] | None]] = []
        for sku in portion_skus:
            code = (sku.get("code") or "").strip().lower()
            sku_id = (sku.get("id") or "").strip().lower()
            matched_item = code_to_item.get(code) or code_to_item.get(sku_id)
            if not matched_item:
                msg = f"Portion SKU code '{code}' not matched to ingredient; skipping."
                append_warning(f"{warn_prefix}: {msg}" if warn_prefix else msg)
                continue
            ordered_items.append((matched_item, sku))
    else:
        ordered_items = [(itm, None) for itm in all_items]

    for item, sku in ordered_items:
        label, name = ingredient_label_and_name(item)
        label = label or name or ""
        if not name:
            continue
        key = _food_key(name)
        food = ensure_food(session, base_url, name, description=label if label != name else None)
        parsed_qty, unit = parse_quantity_and_unit(label, warn_prefix=warn_prefix)
        raw_label_multiplier = _extract_multiplier(label)
        sku_qty = sku.get("quantities", {}).get("in_box") if sku else None
        # If the label has a multiplier (e.g., "x2"), rely on it and ignore the SKU multiplier to avoid double counting.
        if raw_label_multiplier is not None and raw_label_multiplier > 0:
            label_multiplier = raw_label_multiplier
            sku_qty_effective = 1.0 if sku_qty is not None else None
        else:
            label_multiplier = None
            sku_qty_effective = sku_qty
        # Start with the parsed quantity; if we have a multiplier (label or SKU) but no base,
        # assume a base of 1 so the multiplier still takes effect. If we have a SKU entry at all,
        # treat that as at least 1 unit even when the label omits an explicit quantity.
        base_qty: float | None = parsed_qty
        if base_qty is None and (label_multiplier is not None or sku_qty_effective is not None):
            base_qty = 1.0

        if base_qty is not None:
            quantity = float(base_qty)
            if label_multiplier is not None:
                quantity *= float(label_multiplier)
            if sku_qty_effective is not None:
                quantity *= float(sku_qty_effective)
        else:
            quantity = None
        reference_id = _extract_reference_id(item)
        if not reference_id and sku:
            reference_id = _extract_reference_id({"gousto_uuid": sku.get("id")})
        display_text = label or name
        ingredient = {
            "text": display_text,
            "food": food,
            "quantity": quantity,
            "unit": unit,
            "originalText": display_text,
            "note": "",
            "display": display_text,
            "referencedRecipe": None,
        }
        if reference_id:
            ingredient["referenceId"] = reference_id
        merged = False
        if key:
            for existing in ingredients_by_key.get(key, []):
                if _units_compatible(existing.get("unit"), unit):
                    if quantity is not None:
                        existing_qty = existing.get("quantity")
                        existing["quantity"] = (float(existing_qty) if existing_qty is not None else 0.0) + quantity
                    if reference_id and not existing.get("referenceId"):
                        existing["referenceId"] = reference_id
                    merged = True
                    break
        if merged:
            continue
        recipe_ingredients.append(ingredient)
        if key:
            ingredients_by_key.setdefault(key, []).append(ingredient)
    return recipe_ingredients


def _clean_food_name(raw_name: str | None, label: str | None) -> str | None:
    """Strip quantity/packaging tokens from a label to get a reusable food name."""
    text = (label or raw_name or "").strip()
    packaging_prefix: str | None = None
    protein_pattern = r"\b(fillet|fillets|breast|thigh|steak|loin)\b"
    fish_keywords = ("salmon", "haddock", "cod", "trout", "seabass", "sea bass", "bass", "hake", "tuna", "prawn", "prawns", "shrimp")
    meat_keywords = ("chicken", "turkey", "duck", "beef", "pork", "lamb", "meatball", "sausage")
    # Capture pack-style prefixes so we can reattach them after cleaning the base name.
    count_weight_match = re.search(r"(?P<count>\d+(?:\.\d+)?)\s*[x×]\s*(?P<weight>\d+(?:\.\d+)?\s*(?:g|kg|ml|l|cl))", text, flags=re.IGNORECASE)
    if count_weight_match:
        count = count_weight_match.group("count").rstrip()
        weight = count_weight_match.group("weight").strip()
        # Drop the multiplier for certain proteins (e.g., salmon) where we prefer just the pack weight.
        remainder = text.replace(count_weight_match.group(0), "", 1).strip()
        base_lower = remainder.lower()
        if any(k in base_lower for k in fish_keywords) and re.search(protein_pattern, remainder, flags=re.IGNORECASE):
            # For fish fillets/loins, keep just the pack weight.
            packaging_prefix = weight
        elif any(k in base_lower for k in meat_keywords) or re.search(protein_pattern, remainder, flags=re.IGNORECASE):
            # For meat/poultry fillets or generic fillet patterns, keep count x weight.
            packaging_prefix = f"{count} x {weight}"
        text = remainder
    else:
        weight_only_match = re.match(r"(?P<weight>\d+(?:\.\d+)?\s*(?:g|kg|ml|l|cl))\b", text, flags=re.IGNORECASE)
        if weight_only_match:
            remaining = text[len(weight_only_match.group(0)) :].strip()
            # Keep single weight prefixes for proteins where pack size matters.
            base_lower = remaining.lower()
            if any(k in base_lower for k in fish_keywords) and re.search(protein_pattern, remaining, flags=re.IGNORECASE):
                packaging_prefix = weight_only_match.group("weight").strip()
            text = text[len(weight_only_match.group(0)) :].strip()
    size_parenthetical = None
    size_match = re.search(
        r"\(\s*\d+(?:\.\d+)?\s*(?:g|kg|ml|l|cl|oz|lb|lbs|litre|liter)\s*\)",
        text,
        flags=re.IGNORECASE,
    )
    has_tin_or_can = bool(re.search(r"\b(tin|tins|can|cans)\b", text, flags=re.IGNORECASE))
    if size_match:
        size_parenthetical = size_match.group(0).strip()
    if not text:
        return None
    # Remove leading patterns like "2 x 110g".
    text = re.sub(
        r"^[0-9]+(?:\.\d+)?\s*[x×]\s*[0-9]+(?:\.\d+)?\s*(?:g|kg|ml|l|cl|tsp|tbsp|cup|cups|oz|lb|lbs)?\s*",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    # Drop leading quantity/unit markers such as "1", "1x", "15g", "1/2 tsp".
    text = re.sub(
        r"^[0-9]+(?:\.\d+|/[0-9]+)?(?:\s*(?:g|kg|ml|l|cl|tsp|tbsp|cup|cups|oz|lb|lbs)(?![a-zA-Z]))?\s*(?:x\s*)?",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    # Remove trailing multipliers like "x2".
    text = re.sub(r"\s*[x×]\s*\d+(?:\.\d+)?\s*$", "", text, flags=re.IGNORECASE).strip()
    # Remove bracketed packaging/quantity info (e.g., "(200g)") but keep descriptive qualifiers such as "(ready to eat)".
    def _strip_packaging_parenthetical(match: re.Match[str]) -> str:
        content = match.group(1).strip().lower()
        if not content:
            return ""
        # Strip when the parenthetical starts with a number, measurement, or pack-style text.
        if re.match(
            r"^\d+(?:\.\d+)?(?:\s*/\s*\d+(?:\.\d+)?)?\s*(?:g|kg|ml|l|cl|tsp|tbsp|cup|cups|oz|lb|lbs|litre|liter|cm|mm|inch|inches)\b",
            content,
        ):
            return ""
        if re.match(r"^\d+(?:\.\d+)?\s*[x×]\s*\d+", content):
            return ""
        if re.match(
            r"^\d+(?:\.\d+)?\s*(?:pack|packs|packet|packets|bag|bags|pot|pots|tub|tubs|pouch|pouches|tray|trays|tin|tins|can|cans|pc|pcs|piece|pieces)\b",
            content,
        ):
            return ""
        if re.match(r"^\d+(?:\.\d+)?\s*$", content):
            return ""
        return match.group(0)

    text = re.sub(r"\(([^)]*)\)", _strip_packaging_parenthetical, text).strip()
    # Drop packaging words at the end to keep the core ingredient name.
    text = re.sub(
        r"\b(sachet|sachets|packet|packets|pack|packs|bag|bags|pot|pots|tub|tubs|pouch|pouches|tray|trays)\b\.?$",
        "",
        text,
        flags=re.IGNORECASE,
    ).strip()
    # Strip any remaining leading quantity/unit tokens (handle repeated numbers like \"1 1 orange\").
    while True:
        new_text = re.sub(
            r"^(?:\d+(?:\.\d+)?(?:\s*/\s*\d+(?:\.\d+)?)?(?:\s*(?:g|kg|ml|l|cl|tsp|tbsp|cup|cups|oz|lb|lbs|litre|liter|cm|mm|inch|inches)(?![a-zA-Z]))?(?:\s*[x×]\s*)?)",
            "",
            text,
            flags=re.IGNORECASE,
        ).strip()
        if new_text == text:
            break
        text = new_text
    text = re.sub(r"\s+", " ", text)
    if has_tin_or_can and size_parenthetical and size_parenthetical.lower() not in text.lower():
        text = f"{text} {size_parenthetical}".strip()
    if packaging_prefix:
        text = f"{packaging_prefix} {text}".strip()
    if text:
        return text[0].upper() + text[1:]
    if raw_name:
        cleaned = str(raw_name).strip()
        return cleaned[0].upper() + cleaned[1:] if cleaned else None
    return None


def _extract_reference_id(item: dict[str, Any]) -> str | None:
    """Return a valid UUID string for referenceId if present, else None."""
    raw = item.get("uid") or item.get("gousto_uuid")
    if not raw:
        return None
    # Basic UUID v4 style check; Mealie expects a proper UUID format.
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", raw):
        return raw
    return None


def _extract_multiplier(label: str | None) -> float | None:
    """Return a trailing multiplier (e.g., 'x2') if present."""
    if not label:
        return None
    match = re.search(r"[x×]\s*(\d+(?:\.\d+)?)\s*$", label, flags=re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def parse_quantity_and_unit(label: str, warn_prefix: str | None = None) -> tuple[float | None, dict[str, Any] | None]:
    """Parse quantity and unit from a label string, returning the unit dict (or stub) expected by Mealie."""
    if not label:
        return None, None

    label_multiplier = _extract_multiplier(label)
    unit_aliases = {
        "g": "gram",
        "gram": "gram",
        "grams": "gram",
        "kg": "kilogram",
        "kilogram": "kilogram",
        "kilograms": "kilogram",
        "ml": "milliliter",
        "milliliter": "milliliter",
        "milliliters": "milliliter",
        "l": "liter",
        "liter": "liter",
        "litre": "liter",
        "liters": "liter",
        "litres": "liter",
        "tsp": "teaspoon",
        "teaspoon": "teaspoon",
        "teaspoons": "teaspoon",
        "tbsp": "tablespoon",
        "tablespoon": "tablespoon",
        "tablespoons": "tablespoon",
        "clove": "clove",
        "cloves": "clove",
        "pinch": "pinch",
        "pinches": "pinch",
        "cup": "cup",
        "cups": "cup",
        "pack": "pack",
        "packet": "pack",
        "packets": "pack",
        "tin": "tin",
        "tins": "tin",
        "can": "can",
        "cans": "can",
        "cm": "centimeter",
        "slice": "slice",
        "slices": "slice",
        "piece": "piece",
        "pieces": "piece",
        "bunch": "bunch",
        "bunches": "bunch",
    }

    qty: float | None = None
    unit_token = None
    lock_quantity = False  # When True, skip fallback parsing that would override count-based qty.
    count_multiplier: float | None = None
    multiplier_applied = False

    # Handle patterns like "2 x 110g salmon fillets" up front.
    multi_match = re.search(
        r"(?P<count>\d+(?:\.\d+)?)\s*[x×]\s*(?P<num>\d+(?:\.\d+)?)(?P<unit>[a-zA-Z]+)",
        label,
        flags=re.IGNORECASE,
    )
    if multi_match:
        try:
            count = float(multi_match.group("count"))
            # We treat this as "count" items, leaving the per-item weight in the label for readability.
            qty = count
            unit_token = multi_match.group("unit") if "salmon" in label.lower() else None
            lock_quantity = True
        except ValueError:
            qty = None
            unit_token = None

    # If a label multiplier exists alongside a parenthesized weight/volume, capture the base weight
    # and let the caller decide whether to apply the multiplier to avoid double counting.
    if not lock_quantity and label_multiplier is not None:
        paren_match = re.search(r"\((?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]+)\)", label)
        if paren_match:
            try:
                qty = float(paren_match.group("num"))
                unit_token = paren_match.group("unit")
                lock_quantity = True
            except ValueError:
                pass

    parts = label.strip().split()
    if qty is None and parts:
        first = parts[0].rstrip(",").replace("×", "x")
        if first.endswith("x") and len(first) > 1:
            first = first[:-1]
        match = re.match(r"^(?P<num>\d+(?:\.\d+)?)(?P<unit>[a-zA-Z]+)$", first)
        if match:
            try:
                qty = float(match.group("num"))
            except ValueError:
                qty = None
            unit_token = match.group("unit")
        else:
            try:
                # handle simple fractions like 1/2
                if "/" in first:
                    num, denom = first.split("/", 1)
                    qty = float(num) / float(denom)
                else:
                    qty = float(first)
                if len(parts) > 1:
                    unit_token = parts[1].rstrip(",")
            except (ValueError, ZeroDivisionError):
                qty = None
        if not lock_quantity and qty is not None and (unit_token is None or unit_token.lower() not in unit_aliases):
            count_multiplier = qty

    # If we parsed a token that doesn't look like a unit, drop it so bracketed quantities can override.
    if unit_token and unit_token.lower() not in unit_aliases:
        if count_multiplier is None and qty is not None:
            count_multiplier = qty
        unit_token = None

    # Prefer explicit bracketed quantity/unit e.g., "(15g)" anywhere in the label.
    if not lock_quantity and (qty is None or unit_token is None):
        match = re.search(r"\((?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]+)\)", label)
        if match:
            try:
                qty = float(match.group("num"))
            except ValueError:
                qty = None
            unit_token = match.group("unit")
            if count_multiplier is not None and qty is not None and count_multiplier != 1:
                qty *= count_multiplier
                multiplier_applied = True
    # Fallback: search anywhere in the label for a number+unit combo.
    if not lock_quantity and (qty is None or unit_token is None):
        match = re.search(r"(?P<num>\d+(?:\.\d+)?)\s*(?P<unit>[a-zA-Z]+)", label)
        if match:
            try:
                qty = float(match.group("num"))
            except ValueError:
                qty = None
            unit_token = match.group("unit")
    if (
        not lock_quantity
        and count_multiplier is not None
        and qty is not None
        and not multiplier_applied
        and qty != count_multiplier
    ):
        qty *= count_multiplier

    resolved_unit: dict[str, Any] | None = None
    if unit_token:
        alias = unit_aliases.get(unit_token.lower())
        if alias:
            key = _unit_key(alias)
            with CACHE_LOCK:
                if key:
                    resolved_unit = UNITS_BY_KEY.get(key)
                if not resolved_unit:
                    alt_key = _unit_key(unit_token)
                    if alt_key:
                        resolved_unit = UNITS_BY_KEY.get(alt_key)
        # If unit token isn't in our alias map at all, treat as no-unit (e.g., "1 lemon").
        else:
            unit_token = None

        if unit_token and (resolved_unit is None or not resolved_unit.get("id")):
            msg = f"Unit not found for '{label}' (token='{unit_token}'); omitting unit."
            append_warning(f"{warn_prefix}: {msg}" if warn_prefix else msg)
            resolved_unit = None

    return qty, resolved_unit


def _units_compatible(unit_a: dict[str, Any] | None, unit_b: dict[str, Any] | None) -> bool:
    """Return True when two units represent the same thing (including lack of unit)."""
    if unit_a is None and unit_b is None:
        return True
    if not unit_a or not unit_b:
        return False
    id_a = unit_a.get("id")
    id_b = unit_b.get("id")
    if id_a and id_b:
        return id_a == id_b
    if id_a or id_b:
        return False
    for key in ("name", "abbreviation", "pluralName", "pluralAbbreviation"):
        a_value = _unit_key(unit_a.get(key))
        b_value = _unit_key(unit_b.get(key))
        if a_value and b_value and a_value == b_value:
            return True
    return False


def _best_image(images: list[dict[str, Any]]) -> tuple[str, str] | None:
    """Pick (filename, url) for the widest image in a collection."""
    best: dict[str, Any] | None = None
    best_width = -1
    for img in images:
        url = img.get("image")
        width = img.get("width") or 0
        if not url:
            continue
        if width > best_width:
            best = img
            best_width = width
    if not best:
        return None
    url = best["image"]
    return Path(urlparse(url).path).name, url


def select_image(entry: dict[str, Any]) -> tuple[str, str] | None:
    """Return (filename, url) for the widest image in the entry, if present."""
    return _best_image(entry.get("media", {}).get("images") or [])


def expected_stored_filename(filename: str) -> str:
    """Slugify the full filename (including extension) and append the extension again to match Mealie storage."""
    path = Path(filename)
    ext = path.suffix.lstrip(".")
    slug_name = slugify(path.name)
    return f"{slug_name}.{ext}" if ext else slug_name


def collect_instruction_assets(ordered_steps: list[dict[str, Any]]) -> list[tuple[int, str, str]]:
    """Collect unique (index, stored_filename, source_filename) for instruction step images in order."""
    assets: list[tuple[int, str, str]] = []
    seen: set[str] = set()
    for idx, step in enumerate(ordered_steps):
        best = _best_image(step.get("media", {}).get("images") or [])
        if not best:
            continue
        source_filename, _url = best
        stored_filename = expected_stored_filename(source_filename)
        if stored_filename in seen:
            continue
        seen.add(stored_filename)
        assets.append((idx, stored_filename, source_filename))
    return assets


def upload_recipe_image(
    session: requests.Session,
    base_url: str,
    slug: str,
    images_dir: Path,
    filename: str,
) -> None:
    """Upload a recipe hero image that has already been downloaded to images_dir."""
    image_path = images_dir / filename
    if not image_path.exists():
        raise RuntimeError(f"{slug}: image not found at {image_path}")

    extension = image_path.suffix.lstrip(".") or "jpg"
    mime_type, _ = mimetypes.guess_type(image_path)
    mime_type = mime_type or "application/octet-stream"
    name = Path(filename).stem
    url = f"{base_url}/recipes/{slug}/image"
    with image_path.open("rb") as img_file:
        resp = session.put(
            url,
            files={
                "image": (filename, img_file, mime_type),
                "extension": (None, extension),
                "name": (None, name),
            },
            timeout=30,
        )
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"{slug}: failed to upload image ({resp.status_code}): {resp.text}")


def asset_exists(session: requests.Session, base_url: str, recipe_id: str, filename: str) -> bool:
    """Check if an asset already exists for a recipe, tolerating backend 500s."""
    url = f"{base_url}/media/recipes/{recipe_id}/assets/{filename}"
    resp = session.get(url, timeout=10)
    content_type = resp.headers.get("content-type", "")
    if resp.status_code == 200 and ("image" in content_type or "octet-stream" in content_type):
        return True
    return False


def upload_recipe_asset(
    session: requests.Session,
    base_url: str,
    slug: str,
    images_dir: Path,
    stored_filename: str,
    source_filename: str,
) -> str:
    """Upload a recipe asset (e.g., instruction image) to Mealie, returning the stored filename."""
    asset_path = images_dir / source_filename
    if not asset_path.exists():
        raise RuntimeError(f"{slug}: asset not found at {asset_path}")

    extension = asset_path.suffix.lstrip(".") or "jpg"
    mime_type, _ = mimetypes.guess_type(asset_path)
    mime_type = mime_type or "application/octet-stream"
    name = source_filename  # UI sends name including extension
    icon = "mdi-file-image"

    url = f"{base_url}/recipes/{slug}/assets"
    with asset_path.open("rb") as asset_file:
        resp = session.post(
            url,
            files={
                "file": (source_filename, asset_file, mime_type),
                "extension": (None, extension),
                "name": (None, name),
                "icon": (None, icon),
            },
            timeout=30,
        )
    if resp.status_code not in (200, 201, 204):
        raise RuntimeError(f"{slug}: failed to upload asset {source_filename} ({resp.status_code}): {resp.text}")

    stored_name = stored_filename
    try:
        stored_name = resp.json().get("fileName") or stored_name
    except Exception:
        pass

    return stored_name


def process_recipe_file(
    session: requests.Session,
    base_url: str,
    images_dir: Path,
    path: Path,
) -> str | None:
    """Process a single recipe file; return an error message when it fails."""
    stage = "read recipe file"
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:
        return f"{path.name}: failed to read ({exc})"

    entry = extract_entry(data)
    recipe_name = entry.get("title")
    slug = slugify(recipe_name) if recipe_name else None
    if not slug:
        return f"{path.name}: canonical slug missing"

    ordered_steps = sorted(entry.get("cooking_instructions") or [], key=lambda s: s.get("order") or 0)
    instruction_assets = collect_instruction_assets(ordered_steps)
    category_titles = gather_category_titles(entry)
    ingredients = gather_ingredients(entry)
    portion_skus = select_portion_skus(entry, portions=2)
    allergen_tag_names = gather_allergen_tags(entry)

    try:
        stage = "resolve categories"
        recipe_categories = (
            [ensure_category(session, base_url, title) for title in category_titles]
            if category_titles
            else None
        )
        stage = "resolve ingredients"
        has_ingredients = bool(ingredients) or bool(portion_skus)
        recipe_ingredients = (
            build_recipe_ingredients(
                session,
                base_url,
                entry,
                portion_skus,
                warn_prefix=path.name,
            )
            if has_ingredients
            else None
        )
        stage = "resolve tags"
        recipe_tags = (
            [ensure_tag(session, base_url, name) for name in allergen_tag_names]
            if allergen_tag_names
            else None
        )

        stage = "fetch recipe"
        recipe = fetch_recipe(session, base_url, slug)
        created_new = False
        if not recipe:
            stage = "create recipe"
            create_recipe(session, base_url, slug)
            stage = "fetch created recipe"
            recipe = fetch_recipe(session, base_url, slug)
            created_new = True

        if not recipe:
            return f"{path.name}: {slug}: failed to fetch recipe after creation"

        recipe_id = recipe.get("id")

        step_asset_map: dict[int, str] = {asset_idx: stored for asset_idx, stored, _src in instruction_assets}
        recipe_instructions = build_instructions(ordered_steps, recipe_id, step_asset_map)

        stage = "update recipe"
        update_recipe(
            session,
            base_url,
            slug,
            recipe,
            entry,
            recipe_categories,
            recipe_instructions,
            recipe_ingredients,
            recipe_tags,
        )

        image_selection = select_image(entry)
        should_upload_image = created_new or not recipe.get("image")
        if image_selection and should_upload_image:
            image_filename, _image_url = image_selection
            stage = "upload recipe image"
            upload_recipe_image(session, base_url, slug, images_dir, image_filename)

        if recipe_id and instruction_assets:
            stage = "sync instruction assets"
            for asset_idx, stored_filename, source_filename in instruction_assets:
                if asset_exists(session, base_url, recipe_id, stored_filename):
                    continue
                upload_recipe_asset(session, base_url, slug, images_dir, stored_filename, source_filename)
                asset_exists(session, base_url, recipe_id, stored_filename)
    except Exception as exc:
        return f"{path.name}: {slug}: {stage}: {exc}"

    return None


def process_recipe_files(
    paths: list[Path],
    session: requests.Session,
    base_url: str,
    images_dir: Path,
    label: str | None = None,
    max_workers: int = 1,
    session_factory: Callable[[], requests.Session] | None = None,
) -> tuple[list[Path], list[str]]:
    """Process recipe files and return (failed_paths, errors)."""
    errors_local: list[str] = []
    failed: list[Path] = []
    total = len(paths)
    if max_workers <= 1 or total <= 1:
        for idx, path in enumerate(paths, start=1):
            if label:
                print(f"[{label} {idx}/{total}] {path.name}")
            else:
                print(f"[{idx}/{total}] {path.name}")
            err = process_recipe_file(session, base_url, images_dir, path)
            if err:
                errors_local.append(err)
                failed.append(path)
        return failed, errors_local

    if session_factory is None:
        raise ValueError("session_factory is required when max_workers > 1")

    thread_local = threading.local()

    def get_session() -> requests.Session:
        sess = getattr(thread_local, "session", None)
        if sess is None:
            sess = session_factory()
            thread_local.session = sess
        return sess

    def worker(index: int, recipe_path: Path) -> tuple[int, Path, str | None]:
        sess = get_session()
        err = process_recipe_file(sess, base_url, images_dir, recipe_path)
        return index, recipe_path, err

    results: list[tuple[int, Path, str | None]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures: list[concurrent.futures.Future[tuple[int, Path, str | None]]] = []
        for idx, path in enumerate(paths, start=1):
            futures.append(executor.submit(worker, idx, path))
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            completed += 1
            _, path, _err = result
            if label:
                print(f"[{label} {completed}/{total}] {path.name}")
            else:
                print(f"[{completed}/{total}] {path.name}")
            results.append(result)

    for _, path, err in sorted(results, key=lambda item: item[0]):
        if err:
            errors_local.append(err)
            failed.append(path)
    return failed, errors_local


def build_session(mealie_token: str) -> requests.Session:
    """Create a requests session with Mealie auth headers."""
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {mealie_token}"})
    return session


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Import Gousto recipe JSON files into Mealie.")
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of concurrent workers (default: 1 or GOUSTO_WORKERS).",
    )
    return parser.parse_args()


def resolve_workers(arg_value: int | None) -> int:
    """Resolve worker count from CLI args or environment variable."""
    if arg_value is not None:
        workers = arg_value
    else:
        env_value = os.getenv("GOUSTO_WORKERS")
        if not env_value:
            workers = 1
        else:
            try:
                workers = int(env_value)
            except ValueError as exc:
                raise RuntimeError(f"GOUSTO_WORKERS must be an integer (got {env_value!r})") from exc
    if workers < 1:
        raise RuntimeError("workers must be >= 1")
    return workers


def main() -> None:
    """Drive import of Gousto recipe JSON files into Mealie."""
    args = parse_args()
    workers = resolve_workers(args.workers)

    output_dir = Path(get_required_env("GOUSTO_OUTPUT_DIR"))
    if not output_dir.exists():
        raise RuntimeError(f"GOUSTO_OUTPUT_DIR does not exist: {output_dir}")
    images_dir = Path(get_required_env("GOUSTO_IMAGES_DIR"))
    if not images_dir.exists():
        raise RuntimeError(f"GOUSTO_IMAGES_DIR does not exist: {images_dir}")

    mealie_base_url = get_required_env("MEALIE_BASE_URL").rstrip("/")
    mealie_token = get_required_env("MEALIE_TOKEN")

    session = build_session(mealie_token)

    load_ingredient_map(Path(__file__).with_name("ingredient_map.json"))
    if INGREDIENT_MAP:
        print(f"Loaded {len(INGREDIENT_MAP)} ingredient name mappings.")

    print("Loading existing categories...")
    load_existing_categories(session, mealie_base_url)
    print(f"Loaded {len(CATEGORIES_BY_KEY)} categories.")

    print("Loading existing units...")
    load_existing_units(session, mealie_base_url)
    print(f"Loaded {len(UNITS_BY_KEY)} units.")

    print("Loading existing foods...")
    load_existing_foods(session, mealie_base_url)
    print(f"Loaded {len(FOODS_BY_KEY)} foods.")

    print("Loading existing tags...")
    load_existing_tags(session, mealie_base_url)
    print(f"Loaded {len(TAGS_BY_KEY)} tags.")

    recipe_files = sorted(p for p in output_dir.glob("*.json") if not p.name.startswith("._"))
    total = len(recipe_files)
    if workers > 1:
        print(f"Processing {total} recipe file(s) with {workers} workers...")
    else:
        print(f"Processing {total} recipe file(s)...")

    failed_paths, errors[:] = process_recipe_files(
        recipe_files,
        session,
        mealie_base_url,
        images_dir,
        max_workers=workers,
        session_factory=lambda: build_session(mealie_token),
    )
    if failed_paths:
        for attempt in range(1, 11):
            print(f"Retrying {len(failed_paths)} errored file(s) (attempt {attempt}/10)...")
            failed_paths, errors[:] = process_recipe_files(
                failed_paths,
                session,
                mealie_base_url,
                images_dir,
                label=f"retry {attempt}",
                max_workers=workers,
                session_factory=lambda: build_session(mealie_token),
            )
            if not failed_paths:
                break

    if warnings:
        print("Warnings encountered:")
        for warn in warnings:
            print(f" - {warn}")

    if errors:
        print("Errors encountered:")
        for err in errors:
            print(f" - {err}")
    else:
        print("No errors encountered.")

if __name__ == "__main__":
    main()
