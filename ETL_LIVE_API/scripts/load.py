from supabase import create_client
import os
import pandas as pd
import time
from dotenv import load_dotenv
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
def load_data_to_supabase():
    data_path = "../data/staged/weather_cleaned.csv"
    df = pd.read_csv(data_path)

    df["time"] = pd.to_datetime(df["time"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    df["extracted_at"] = pd.to_datetime(df["extracted_at"]).dt.strftime('%Y-%m-%d %H:%M:%S')
    batch_size = 20
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i+batch_size].where(pd.notnull(df), None).to_dict("records")

        values = []
        for r in batch:
            values.append(
                f"('{r['time']}', {r.get('temperature_C','NULL')}, "
                f"{r.get('humidity_percent','NULL')}, {r.get('wind_speed_kmph','NULL')}, "
                f"'{r.get('city','NULL')}', '{r['extracted_at']}')"
            )

        insert_sql = f"""
        INSERT INTO weather_data (time, temperature_C, humidity_percent, wind_speed_kmph, city, extracted_at)
        VALUES {",".join(values)};
        """


        supabase.rpc("execute_sql", {"query": insert_sql}).execute()
        print(f"Inserted rows {i+1} --- {min(i+batch_size, len(df))}")
        time.sleep(0.5)
    print("Finished loading weather data.")
if __name__ == "__main__":
    load_data_to_supabase()
