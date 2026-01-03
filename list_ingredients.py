"""List Gousto ingredient names from recipe JSON files and write them to a text file."""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()


def get_required_env(var_name: str) -> str:
    """Return required environment variable or raise if missing."""
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value


def normalize_text(value: Any) -> str | None:
    """Trim and collapse whitespace."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return " ".join(text.split())


def normalize_key(value: str | None) -> str | None:
    """Lowercase key for de-duplication."""
    if value is None:
        return None
    text = value.strip().lower()
    return text or None


def extract_entry(data: dict[str, Any]) -> dict[str, Any]:
    """Pull the nested entry block from a Gousto JSON payload."""
    return data.get("data", {}).get("entry", {})


def gather_ingredients(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Collect ingredients and basics as a single list."""
    ingredients: list[dict[str, Any]] = []
    ingredients.extend(entry.get("ingredients") or [])
    ingredients.extend(entry.get("basics") or [])
    return ingredients


def ingredient_name(item: dict[str, Any]) -> str | None:
    """Pick the best human-readable ingredient name from an item."""
    for key in ("label", "name", "title"):
        name = normalize_text(item.get(key))
        if name:
            return name
    return None


def load_entry(path: Path) -> dict[str, Any]:
    """Load a Gousto entry block from a recipe file."""
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return extract_entry(data)


def collect_ingredient_names(
    recipe_paths: list[Path], show_progress: bool = False
) -> tuple[list[str], Counter[str], dict[str, set[str]]]:
    """Return sorted ingredient names, occurrence counts, and files per ingredient."""
    names_by_key: dict[str, str] = {}
    counts: Counter[str] = Counter()
    files_by_key: dict[str, set[str]] = {}

    total = len(recipe_paths)
    progress_stride = max(1, total // 10)  # ~10 updates across the run

    for idx, path in enumerate(recipe_paths, start=1):
        if show_progress and (idx == 1 or idx == total or idx % progress_stride == 0):
            print(f"[{idx}/{total}] {path.name}")
        try:
            entry = load_entry(path)
        except Exception as exc:
            print(f"{path.name}: failed to load ({exc})")
            continue

        for item in gather_ingredients(entry):
            name = ingredient_name(item)
            key = normalize_key(name)
            if not name or not key:
                continue
            names_by_key.setdefault(key, name)
            counts[key] += 1
            files_by_key.setdefault(key, set()).add(path.name)

    sorted_names = sorted(names_by_key.values(), key=lambda n: n.lower())
    return sorted_names, counts, files_by_key


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List normalized Gousto ingredient names from recipe JSON files."
    )
    parser.add_argument(
        "--dir",
        type=Path,
        help="Directory containing Gousto recipe JSON files (defaults to $GOUSTO_OUTPUT_DIR)",
    )
    parser.add_argument("--counts", action="store_true", help="Include how many times each ingredient appears.")
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Disable per-file progress output (enabled by default).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("ingredients.txt"),
        help="Path to write the ingredient list text file (default: ingredients.txt in CWD).",
    )
    parser.add_argument(
        "--output-with-files",
        type=Path,
        default=Path("ingredients_with_files.txt"),
        help="Path to write ingredient list with the JSON file names for each ingredient (default: ingredients_with_files.txt).",
    )
    args = parser.parse_args()

    output_dir = args.dir or Path(get_required_env("GOUSTO_OUTPUT_DIR"))
    if not output_dir.exists():
        raise SystemExit(f"Recipe directory does not exist: {output_dir}")

    recipe_paths = sorted(p for p in output_dir.glob("*.json") if not p.name.startswith("._"))
    if not recipe_paths:
        raise SystemExit(f"No recipe JSON files found in {output_dir}")

    names, counts, files_by_key = collect_ingredient_names(
        recipe_paths, show_progress=not args.no_progress
    )

    lines: list[str] = []
    detailed_lines: list[str] = []
    for name in names:
        if args.counts:
            key = normalize_key(name)
            count = counts.get(key or "", 0)
            line = f"{name} ({count})"
        else:
            key = normalize_key(name)
            line = name
        lines.append(line)

        file_list = sorted(files_by_key.get(key or "", []))
        if file_list:
            detailed_lines.append(f"{line}: {', '.join(file_list)}")
        else:
            detailed_lines.append(line)

    output_path = args.output
    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    files_output_path = args.output_with_files
    files_output_path.write_text("\n".join(detailed_lines) + "\n", encoding="utf-8")
    print(
        f"Wrote {len(names)} unique ingredients to {output_path} "
        f"and {files_output_path} from {len(recipe_paths)} recipe files."
    )


if __name__ == "__main__":
    main()
