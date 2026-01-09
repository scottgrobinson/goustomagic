# Goustomagic

Scripts for pulling Gousto recipe data, importing it into a Mealie instance, and checking ingredient consistency.

## Setup
- Python 3.13+.
- Install dependencies (choose one):
  - `uv sync`
  - `uv pip install requests python-dotenv python-slugify beautifulsoup4 unicode`
- Environment variables (can be stored in a `.env`):
  - `GOUSTO_OUTPUT_DIR`: folder where Gousto recipe JSON files are stored.
  - `GOUSTO_IMAGES_DIR`: folder where Gousto images are stored (downloaded and read for Mealie uploads).
  - `MEALIE_BASE_URL`: base URL of your Mealie instance (e.g., `https://mealie.example.com/api`).
  - `MEALIE_TOKEN`: API token with rights to read/write recipes, foods, units, categories, and tags.
  - `GOUSTO_WORKERS`: optional default worker count for recipe imports (overridden by `--workers`).

## Scripts
- `download_recipes.py`: fetches all Gousto recipes and saves JSON to `GOUSTO_OUTPUT_DIR`, downloading 1500px hero and step images to `GOUSTO_IMAGES_DIR`. Runs as `uv run download_recipes.py`.
- `import_to_mealie.py`: imports every recipe JSON in `GOUSTO_OUTPUT_DIR` into Mealie, creating categories/tags/units/foods as needed and uploading images from `GOUSTO_IMAGES_DIR`. Uses `ingredient_map.json` if present to normalize food names. Runs as `uv run import_to_mealie.py` (add `--workers 4` or set `GOUSTO_WORKERS` for parallel imports).
- `export_mealie_ingredients.py`: fetches all foods (ingredients) from Mealie and writes a text file. Example: `uv run export_mealie_ingredients.py --include-slugs --include-ids --output mealie_ingredients.txt`.
- `verify_mealie_ingredients.py`: compares Mealie recipe ingredients against a static expectation list (default `expected_ingredients.json`). Example: `uv run verify_mealie_ingredients.py --recipe veggie-lasagne --tolerance 0.05` (supports `--recipes-file` and `--allow-missing-expected`).
- `delete_mealie_data.py`: deletes all recipes and foods from Mealie (dry-run supported). Example: `uv run delete_mealie_data.py --dry-run` (add `--force` to skip confirmation).

## Data files
- `expected_ingredients.json`: expected ingredient lists keyed by recipe slug, used by `verify_mealie_ingredients.py`. Format: `{ "recipe-slug": [{"name": "...", "quantity": 1, "unit": "gram"}, ...] }` (quantity/unit may be null).
- `ingredient_map.json`: optional mapping of raw Gousto ingredient names to normalized names during import.
- `mealie_ingredients.txt`: optional export output from `export_mealie_ingredients.py`.

## Tips
- Run `download_recipes.py` before importing so the JSON data exists locally.
- `import_to_mealie.py` is idempotent: it updates existing recipes when Gousto data changes.
- `download_recipes.py` runs with DEBUG logging in `__main__`; change `log_level` to `logging.INFO` if you want quieter output.
