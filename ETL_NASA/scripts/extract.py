import json
from pathlib import Path
from datetime import datetime
import requests

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

def extract_nasa_data():
    url="https://api.nasa.gov/planetary/apod?api_key=P3Io061P1EBN5AbXPZE7LzN1zFexImO02ClpmjIB"
    params={
        "date": datetime.now().strftime('%Y-%m-%d')
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()

    filename = DATA_DIR/f"nasa_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(data, indent=2))
    print(f"Extracted data saved to : {filename}")
    return data
if __name__ == "__main__":
    extract_nasa_data()