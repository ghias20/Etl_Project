import pandas as pd
import json
import glob
import os
def transform_nasa_data():
    os.makedirs("../data/staged",exist_ok=True)
    latest_file=sorted(glob.glob("../data/raw/nasa_*.json"))[-1]
    with open(latest_file,'r') as f:
        data=json.load(f)
    df=pd.DataFrame({
        "date":[data.get("date")],
        "title":[data.get("title")],
        "explanation":[data.get("explanation")],
        "media_type":[data.get("media_type")],
        "image_url":[data.get("url") if data.get("media_type")=="image" else None],
        "inserted_at":[pd.Timestamp.now()]
    })
    output_path="../data/staged/nasa_cleaned.csv"
    df.to_csv(output_path,index=False)
    print(f"Transformed NASA records saved to :{output_path}")
    return df
if __name__=="__main__":
    transform_nasa_data()