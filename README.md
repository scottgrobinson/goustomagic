# Goustomagic

Scripts for pulling Gousto recipe data, importing it into a Mealie instance, and checking ingredient consistency.

## Setup
- Python 3.13+.
- Install dependencies (choose one):
  - `uv pip install requests python-dotenv python-slugify beautifulsoup4 unicode`
- Environment variables (can be stored in a `.env`):
  - `GOUSTO_OUTPUT_DIR`: folder where Gousto recipe JSON files are stored.
  - `GOUSTO_IMAGES_DIR`: folder where Gousto images are stored (downloaded and read for Mealie uploads).
  - `MEALIE_BASE_URL`: base URL of your Mealie instance (e.g., `https://mealie.example.com/api`).
  - `MEALIE_TOKEN`: API token with rights to read/write recipes, foods, units, categories, and tags.

## Scripts
- `downloadrecipes.py`: fetches all Gousto recipes and saves JSON to `GOUSTO_OUTPUT_DIR`, downloading hero and step images to `GOUSTO_IMAGES_DIR`. Runs as `uv run downloadrecipes.py`.
- `import_to_mealie.py`: imports every recipe JSON in `GOUSTO_OUTPUT_DIR` into Mealie, creating categories/tags/units/foods as needed and uploading images from `GOUSTO_IMAGES_DIR`. Runs as `uv run import_to_mealie.py`.
- `list_ingredients.py`: normalizes ingredient names across all recipe JSON files and writes `ingredients.txt` plus `ingredients_with_files.txt`. Example: `uv run list_ingredients.py --counts`.
- `verify_mealie_ingredients.py`: compares Mealie recipe ingredients against a static expectation list (default `expected_ingredients.json`). Example: `uv run verify_mealie_ingredients.py --recipe veggie-lasagne`.
- `delete_mealie_data.py`: deletes all recipes and foods from Mealie (dry-run supported). Example: `uv run delete_mealie_data.py --dry-run`.

## Data files
- `expected_ingredients.json`: expected ingredient lists keyed by recipe slug, used by `verify_mealie_ingredients.py`.

## Tips
- Run `downloadrecipes.py` before listing or importing so the JSON data exists locally.
- `import_to_mealie.py` is idempotent: it updates existing recipes when Gousto data changes.
- Keep an eye on `downloadrecipes.py` logs; setting `log_level=logging.DEBUG` in the `__main__` block prints progress for each recipe.
