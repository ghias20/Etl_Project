from supabase import create_client
import os
import pandas as pd
import time
from dotenv import load_dotenv

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def load_nasa_data_to_supabase():
    csv_path = "../data/staged/nasa_cleaned.csv"   # FIXED to match transform.py output
    if not os.path.exists(csv_path):
        print(f"File {csv_path} does not exist. Please run the transform step first.")
        return

    df = pd.read_csv(csv_path)

    # Clean datetime
    df["inserted_at"] = pd.to_datetime(df["inserted_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    batch_size = 20

    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size].where(pd.notnull(df), None).to_dict("records")

        values = []
        for r in batch:

            # INLINE FIXES (no functions added)
            date = str(r.get("date", "")).replace("'", "''")
            title = str(r.get("title", "") or "").replace("'", "''")
            explanation = str(r.get("explanation", "") or "").replace("'", "''")
            media_type = str(r.get("media_type", "") or "").replace("'", "''")
            image_url = str(r.get("image_url", "") or "").replace("'", "''")

            # REMOVE LINE BREAKS to avoid SQL errors
            explanation = explanation.replace("\n", " ").replace("\r", " ")

            values.append(
                f"('{date}', '{title}', '{explanation}', '{media_type}', '{image_url}')"
            )

        if not values:
            continue

        insert_query = f"""
        INSERT INTO nasa_apod (date, title, explanation, media_type, image_url)
        VALUES {",".join(values)};
        """

        supabase.rpc("execute_sql", {"query": insert_query}).execute()
        print(f"Inserted batch {i // batch_size + 1} into Supabase")
        time.sleep(0.5)

    print("NASA APOD data loading to Supabase completed.")

if __name__ == "__main__":
    load_nasa_data_to_supabase()
