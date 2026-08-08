import json
from pathlib import Path
from datetime import datetime


def save_json(data, prefix):
    folder = Path("../data/raw")
    folder.mkdir(parents=True, exist_ok=True)
    timpstamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    filename = folder / f"{prefix}_{timpstamp}.json"

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)

    return filename

