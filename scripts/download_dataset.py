from __future__ import annotations

import argparse
import shutil
import urllib.request
from pathlib import Path

DATASET_URL = "https://huggingface.co/datasets/criteo/criteo-attribution-dataset/resolve/main/criteo_attribution_dataset.tsv.gz"


def main() -> None:
    parser = argparse.ArgumentParser(description="Download the Criteo attribution dataset from Hugging Face.")
    parser.add_argument("--output-dir", default="data")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        from huggingface_hub import hf_hub_download

        downloaded = hf_hub_download(
            repo_id="criteo/criteo-attribution-dataset",
            repo_type="dataset",
            filename="criteo_attribution_dataset.tsv.gz",
            local_dir=output_dir,
            local_dir_use_symlinks=False,
        )
    except ModuleNotFoundError:
        target = output_dir / "criteo_attribution_dataset.tsv.gz"
        with urllib.request.urlopen(DATASET_URL) as response, target.open("wb") as fp:
            shutil.copyfileobj(response, fp)
        downloaded = str(target)
    print(downloaded)


if __name__ == "__main__":
    main()
