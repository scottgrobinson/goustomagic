"""
Convert Gousto recipe JSON into schema.org Recipe JSON-LD.

Usage:
  python gousto_to_schema.py path/to/recipe.json              # prints JSON-LD to stdout
  python gousto_to_schema.py path/*.json --output-dir schema  # writes *.schema.json files

Notes:
- Warns about missing or suspicious fields; optionally embeds them via --annotate-issues.
- Exits non-zero if an input cannot be read or parsed.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bs4 import BeautifulSoup

GOUSTO_BASE_URL = "https://www.gousto.co.uk"


def html_to_text(value: str) -> str:
    """
    Convert Gousto HTML snippets into plain text.
    """
    soup = BeautifulSoup(value or "", "html.parser")
    return soup.get_text(" ", strip=True)


def normalize_text(value: Optional[str]) -> str:
    if not value:
        return ""
    text = html_to_text(value)
    text = text.replace("¶ÿ", " ").replace("\xa0", " ").replace("ƒ?", "'")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def iso_duration_minutes(value: Optional[int]) -> Optional[str]:
    if value in (None, "", 0):
        return None
    return f"PT{int(value)}M"


def mg_to_grams(value: Optional[float]) -> Optional[str]:
    if value in (None, "", 0):
        return None
    grams = round(float(value) / 1000, 1)
    return f"{grams} g"


def pick_images(media: Dict[str, Any]) -> List[str]:
    images = media.get("images") or []
    sorted_imgs = sorted(images, key=lambda img: img.get("width") or 0, reverse=True)
    urls = []
    for img in sorted_imgs:
        url = img.get("image")
        if url:
            urls.append(url)
    return urls


def pick_step_image(media: Dict[str, Any]) -> Optional[str]:
    candidates = pick_images(media)
    return candidates[0] if candidates else None


def collect_ingredients(entry: Dict[str, Any]) -> List[str]:
    ingredients = [ing.get("label") for ing in entry.get("ingredients", []) if ing.get("label")]
    basics = [b.get("title") for b in entry.get("basics", []) if b.get("title")]
    return [item for item in (ingredients + basics) if item]


def collect_instructions(entry: Dict[str, Any], issues: List[str]) -> List[Dict[str, Any]]:
    steps = []
    raw_steps = sorted(entry.get("cooking_instructions", []), key=lambda s: s.get("order", 0))
    for idx, step in enumerate(raw_steps, start=1):
        text = normalize_text(step.get("instruction"))
        if not text:
            issues.append(f"Step {idx} is empty")
            continue
        step_data: Dict[str, Any] = {"@type": "HowToStep", "position": idx, "text": text}
        if img := pick_step_image(step.get("media") or {}):
            step_data["image"] = img
        steps.append(step_data)
    return steps


def collect_nutrition(entry: Dict[str, Any]) -> Dict[str, Any]:
    per_portion = entry.get("nutritional_information", {}).get("per_portion", {})
    if not per_portion:
        return {}
    nutrition: Dict[str, Any] = {
        "@type": "NutritionInformation",
        "calories": f"{per_portion.get('energy_kcal')} kcal" if per_portion.get("energy_kcal") else None,
        "carbohydrateContent": mg_to_grams(per_portion.get("carbs_mg")),
        "fatContent": mg_to_grams(per_portion.get("fat_mg")),
        "saturatedFatContent": mg_to_grams(per_portion.get("fat_saturates_mg")),
        "fiberContent": mg_to_grams(per_portion.get("fibre_mg")),
        "proteinContent": mg_to_grams(per_portion.get("protein_mg")),
        "sodiumContent": mg_to_grams(per_portion.get("salt_mg")),
        "sugarContent": mg_to_grams(per_portion.get("carbs_sugars_mg")),
    }
    return {k: v for k, v in nutrition.items() if v}


def collect_categories(entry: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    if cuisine := entry.get("cuisine", {}).get("title"):
        names.append(cuisine)
    for cat in entry.get("categories", []):
        if title := cat.get("title"):
            names.append(title)
    return sorted(set(names))


def collect_keywords(entry: Dict[str, Any]) -> List[str]:
    tags = [t.get("title") or t.get("slug") for t in entry.get("tags", []) if t]
    allergens = [a.get("title") or a.get("slug") for a in entry.get("allergens", []) if a]
    basics = [b.get("title") or b.get("slug") for b in entry.get("basics", []) if b]
    return [t for t in (tags + allergens + basics) if t]


def canonical_url(entry: Dict[str, Any]) -> Optional[str]:
    if url := entry.get("seo", {}).get("canonical"):
        return url
    if path := entry.get("url"):
        return f"{GOUSTO_BASE_URL}{path}"
    return None


def build_recipe(entry: Dict[str, Any], slug: str, issues: List[str]) -> Dict[str, Any]:
    if not entry:
        raise ValueError("Entry payload is empty")

    name = entry.get("title") or slug
    description = normalize_text(entry.get("description"))
    if not description:
        issues.append("Description is missing")

    images = pick_images(entry.get("media", {}))
    if not images:
        issues.append("No main images found")

    prep_minutes = entry.get("prep_times", {}).get("for_2")
    total_time = iso_duration_minutes(prep_minutes)
    if total_time is None:
        issues.append("Prep/total time missing")

    instructions = collect_instructions(entry, issues)
    if not instructions:
        issues.append("No instructions found")

    ingredients = collect_ingredients(entry)
    if not ingredients:
        issues.append("No ingredients found")

    recipe: Dict[str, Any] = {
        "@context": "https://schema.org",
        "@type": "Recipe",
        "name": name,
        "description": description,
        "url": canonical_url(entry),
        "mainEntityOfPage": canonical_url(entry),
        "image": images,
        "thumbnailUrl": images[0] if images else None,
        "recipeYield": "2 servings",
        "recipeIngredient": ingredients,
        "recipeInstructions": instructions,
        "recipeCategory": collect_categories(entry) or None,
        "recipeCuisine": entry.get("cuisine", {}).get("title"),
        "keywords": collect_keywords(entry) or None,
        "prepTime": total_time,
        "totalTime": total_time,
        "author": {"@type": "Organization", "name": "Gousto"},
        "nutrition": collect_nutrition(entry) or None,
        "isBasedOn": canonical_url(entry),
    }

    return {k: v for k, v in recipe.items() if v not in (None, [], {})}


def load_entry(path: Path) -> Tuple[Dict[str, Any], List[str]]:
    issues: List[str] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Failed to read {path}: {exc}") from exc
    entry = data.get("data", {}).get("entry", {})
    if not entry:
        issues.append("Missing data.entry block")
    return entry, issues


def write_output(recipe: Dict[str, Any], out_path: Optional[Path], pretty: bool) -> None:
    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(recipe, f, ensure_ascii=False, indent=2 if pretty else None)
    else:
        json.dump(recipe, sys.stdout, ensure_ascii=False, indent=2 if pretty else None)
        sys.stdout.write("\n")


def process_file(
    path: Path,
    output_dir: Optional[Path],
    pretty: bool,
    annotate_issues: bool,
    logger: logging.Logger,
) -> int:
    entry, issues = load_entry(path)
    slug = path.stem
    try:
        recipe = build_recipe(entry, slug, issues)
    except Exception as exc:  # noqa: BLE001
        logger.error("Unable to convert %s: %s", path.name, exc)
        return 1

    if annotate_issues and issues:
        recipe["_conversion_issues"] = issues

    out_path = None
    if output_dir:
        out_path = output_dir / f"{slug}-schema.json"
    write_output(recipe, out_path, pretty)

    if issues:
        joined = "; ".join(issues)
        if out_path:
            logger.warning("%s: conversion issues: %s", out_path.name, joined)
        else:
            logger.warning("%s: conversion issues: %s", path.name, joined)
    else:
        target = out_path.name if out_path else path.name
        logger.info("%s: converted with no issues detected", target)
    return 0


def expand_inputs(inputs: Iterable[str]) -> List[Path]:
    paths: List[Path] = []
    for pattern in inputs:
        matches = sorted(Path().glob(pattern))
        if not matches:
            raise FileNotFoundError(f"No files match pattern {pattern}")
        paths.extend(matches)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert Gousto recipe JSON into schema.org Recipe JSON-LD")
    parser.add_argument("inputs", nargs="+", help="One or more Gousto recipe JSON files (globs allowed)")
    parser.add_argument("--output-dir", help="Write converted files to this directory (otherwise print to stdout)")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument(
        "--annotate-issues",
        action="store_true",
        help="Embed detected conversion issues under _conversion_issues",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO)")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper(), format="%(levelname)s %(message)s")
    logger = logging.getLogger("gousto_to_schema")

    if len(args.inputs) > 1 and not args.output_dir:
        parser.error("Multiple inputs require --output-dir to avoid mixing outputs")

    output_dir = Path(args.output_dir).expanduser() if args.output_dir else None

    try:
        paths = expand_inputs(args.inputs)
    except Exception as exc:  # noqa: BLE001
        logger.error("%s", exc)
        sys.exit(1)

    exit_code = 0
    for path in paths:
        exit_code |= process_file(path, output_dir, args.pretty, args.annotate_issues, logger)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
