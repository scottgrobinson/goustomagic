"""Download Gousto recipe JSON and images to local folders, skipping unchanged files."""

import os
import json
import logging
from typing import List, Tuple, Dict, Optional

import requests
from dotenv import load_dotenv

load_dotenv()


def get_required_env(var_name: str) -> str:
    """Return required environment variable or raise if missing."""
    value = os.getenv(var_name)
    if not value:
        raise RuntimeError(f"Environment variable {var_name} is required")
    return value


def download_image(url: str, folder: str, session: Optional[requests.Session] = None) -> None:
    """
    Download a single image from `url` into `folder`, preserving the filename.
    """
    sess = session or requests
    filename = url.split('/')[-1]
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    try:
        resp = sess.get(url, stream=True)
        resp.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in resp.iter_content(1024):
                f.write(chunk)
        return
    except Exception as e:
        raise RuntimeError(f"Failed to download image {url}: {e}")


class GoustoRecipeSync:
    """Fetch Gousto recipes, persist JSON, and download associated images."""
    BASE_LIST_URL = "https://production-api.gousto.co.uk/cmsreadbroker/v1/recipes"
    BASE_DETAIL_URL = "https://production-api.gousto.co.uk/cmsreadbroker/v1/recipe/"

    def __init__(
        self,
        output_dir: str,
        images_dir: str,
        batch_size: int = 16,
        log_level: int = logging.INFO
    ) -> None:
        # Configure logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(handler)

        self.session = requests.Session()
        self.output_dir = output_dir
        self.images_dir = images_dir
        self.batch_size = batch_size
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.images_dir, exist_ok=True)

        self.logger.debug(f"Initialized with output_dir={output_dir}, images_dir={images_dir}, batch_size={batch_size}")

    def fetch_all_recipe_urls(self) -> List[str]:
        """
        Fetch all Gousto recipe URL paths in batches.
        """
        self.logger.info("Fetching total recipe count...")
        params = {"category": "recipes", "limit": 1, "offset": 0}
        resp = self.session.get(self.BASE_LIST_URL, params=params)
        resp.raise_for_status()
        total = resp.json()["data"].get("count", 0)
        self.logger.info(f"Total recipes to fetch: {total}")

        urls: List[str] = []
        for offset in range(0, total, self.batch_size):
            self.logger.debug(f"Fetching recipes {offset + 1} to {min(offset + self.batch_size, total)}...")
            params = {"category": "recipes", "limit": self.batch_size, "offset": offset}
            resp = self.session.get(self.BASE_LIST_URL, params=params)
            resp.raise_for_status()
            entries = resp.json()["data"].get("entries", [])
            for entry in entries:
                if url := entry.get("url"):
                    urls.append(url)
        self.logger.info(f"Fetched {len(urls)} recipe URLs")
        return urls

    def fetch_recipe_detail(self, slug: str) -> Dict:
        """
        Fetch the detailed recipe JSON for a given slug.
        """
        self.logger.debug(f"Fetching details for recipe: {slug}")
        resp = self.session.get(self.BASE_DETAIL_URL + slug)
        resp.raise_for_status()
        return resp.json()

    def has_changed(self, slug: str, new_data: Dict) -> bool:
        """
        Compare fetched data with existing file, if present.
        Returns True if data is new or has changed.
        """
        path = os.path.join(self.output_dir, f"{slug}.json")
        if not os.path.exists(path):
            self.logger.debug(f"Recipe {slug} is new (no existing file)")
            return True
        try:
            with open(path, 'r', encoding='utf-8') as f:
                old_data = json.load(f)
            changed = old_data != new_data
            self.logger.debug(f"Recipe {slug} change detected: {changed}")
            return changed
        except (OSError, json.JSONDecodeError) as e:
            self.logger.warning(f"Could not read existing file for {slug}, treating as changed: {e}")
            return True

    def save_recipe(self, slug: str, data: Dict) -> None:
        """
        Save recipe JSON to disk.
        """
        path = os.path.join(self.output_dir, f"{slug}.json")
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        self.logger.debug(f"Saved recipe JSON to {path}")

    def download_images_for_recipe(self, slug: str, data: Dict) -> None:
        """
        Extract and download all 1500px images for a recipe's media and cooking steps.
        """
        entry = data.get("data", {}).get("entry", {})
        # 1. Main media images
        for img in entry.get("media", {}).get("images", []):
            if img.get("width") == 1500 and (url := img.get("image")):
                try:
                    download_image(url, self.images_dir, session=self.session)
                    self.logger.debug(f"Downloaded main image for {slug}: {url}")
                except Exception as e:
                    self.logger.error(f"{slug}: image download error: {e}")
                break
        # 2. Step images
        for step in entry.get("cooking_instructions", []):
            for img in step.get("media", {}).get("images", []):
                if img.get("width") == 1500 and (url := img.get("image")):
                    try:
                        download_image(url, self.images_dir, session=self.session)
                        self.logger.debug(f"Downloaded step image for {slug}: {url}")
                    except Exception as e:
                        self.logger.error(f"{slug}: step image download error: {e}")
                    break

    def sync(self) -> Tuple[int, int, int, List[Tuple[str, str]]]:
        """
        Fetch all recipes, detect changes, save as needed, and download images.
        Returns counts: (new, updated, unchanged, failures).
        """
        self.logger.info("Starting sync...")
        new_count = updated_count = unchanged_count = 0
        failures: List[Tuple[str, str]] = []

        urls = self.fetch_all_recipe_urls()
        total = len(urls)
        for idx, recipe_url in enumerate(urls, start=1):
            slug = recipe_url.rstrip('/').split('/')[-1]
            prefix = f"[{idx}/{total}]"
            try:
                data = self.fetch_recipe_detail(slug)
            except Exception as e:
                msg = f"fetch error: {e}"
                self.logger.error(f"{prefix} {slug}: {msg}")
                failures.append((slug, msg))
                continue

            try:
                if self.has_changed(slug, data):
                    first_time = not os.path.exists(os.path.join(self.output_dir, f"{slug}.json"))
                    self.save_recipe(slug, data)
                    self.download_images_for_recipe(slug, data)
                    if first_time:
                        new_count += 1
                        self.logger.info(f"{prefix} [NEW] {slug}")
                    else:
                        updated_count += 1
                        self.logger.info(f"{prefix} [UPDATED] {slug}")
                else:
                    unchanged_count += 1
                    self.logger.debug(f"{prefix} [UNCHANGED] {slug}")
            except Exception as e:
                msg = f"processing error: {e}"
                self.logger.error(f"{prefix} {slug}: {msg}")
                failures.append((slug, msg))

        total_checked = new_count + updated_count + unchanged_count
        self.logger.info(f"Sync complete: {total_checked} recipes ({new_count} new, {updated_count} updated, {unchanged_count} unchanged)")
        if failures:
            self.logger.warning(f"Failures: {len(failures)} recipes")
            for slug, msg in failures:
                self.logger.warning(f" - {slug}: {msg}")
        return new_count, updated_count, unchanged_count, failures


if __name__ == "__main__":
    output_dir = get_required_env("GOUSTO_OUTPUT_DIR")
    images_dir = get_required_env("GOUSTO_IMAGES_DIR")

    syncer = GoustoRecipeSync(
        output_dir=output_dir,
        images_dir=images_dir,
        batch_size=16,
        log_level=logging.DEBUG
    )
    syncer.sync()
