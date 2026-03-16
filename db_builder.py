import sqlite3
import pandas as pd
import os

from config import (
    DB_NAME,
    GA4_CSV_PATH as GA4_CSV,
    SITE_URL as BASE_DOMAIN
)

# --- CONFIGURATION ---
# DB_NAME = "seo_master.db"
# GA4_CSV = "data_uploads/ga4_data.csv"
# BASE_DOMAIN = "https://bodycraftacademy.com/"  # MANDATORY: CHANGE THIS TO YOUR ACTUAL WEBSITE




def normalize_url(url_string):
   """Converts GA4 relative paths to absolute URLs and standardizes them."""
   if pd.isna(url_string):
       return None
   url_string = str(url_string).strip()


   # GA4 usually gives relative paths (e.g., /about-us)
   if url_string.startswith('/'):
       url_string = BASE_DOMAIN + url_string


   # Strip trailing slash to prevent duplicate database rows (e.g., /about vs /about/)
   if url_string.endswith('/'):
       url_string = url_string[:-1]


   return url_string




def load_dirty_ga4_csv(filepath):
   """Scans a GA4 CSV to find the real table header using Two-Factor Validation."""
   try:
       with open(filepath, 'r', encoding='utf-8') as f:
           lines = f.readlines()


       header_index = 0
       # Scan the first 20 lines to find the actual table headers
       for i, line in enumerate(lines[:20]):
           # TWO-FACTOR VALIDATION: Must have a keyword AND be a wide table (>= 3 commas)
           if ('Page' in line or 'path' in line or 'URL' in line) and line.count(',') >= 3:
               header_index = i
               break


       print(f"[*] Detected GA4 table starting at row {header_index}. Skipping metadata...")
       return pd.read_csv(filepath, skiprows=header_index)


   except UnicodeDecodeError:
       # Fallback for UTF-16 TSV format
       with open(filepath, 'r', encoding='utf-16') as f:
           lines = f.readlines()
       header_index = 0
       for i, line in enumerate(lines[:20]):
           if ('Page' in line or 'path' in line or 'URL' in line) and line.count('\t') >= 3:
               header_index = i
               break
       print(f"[*] Detected UTF-16 GA4 table starting at row {header_index}. Skipping metadata...")
       return pd.read_csv(filepath, skiprows=header_index, encoding='utf-16', sep='\t')




def build_database():
   print("[*] Initializing SEO Master Database (GA4 Only)...")
   conn = sqlite3.connect(DB_NAME)
   cursor = conn.cursor()


   # Create the Master Table schema required for the full pipeline
   cursor.execute("""
   CREATE TABLE IF NOT EXISTS Pages (
       url TEXT PRIMARY KEY,
       ga4_sessions INTEGER DEFAULT 0,
       scraped_h1 TEXT,
       scraped_text TEXT,
       status_code INTEGER,
       llm_eeat_score INTEGER,
       llm_recommendation TEXT,
       is_scraped BOOLEAN DEFAULT FALSE,
       is_analyzed BOOLEAN DEFAULT FALSE
   )
   """)
   conn.commit()


   if not os.path.exists(GA4_CSV):
       print(f"[!] FATAL ERROR: Could not find {GA4_CSV}.")
       print("    Ensure the file is inside the 'data_uploads' folder.")
       return


   print("[*] Processing Google Analytics 4 (GA4) Data...")
   df_ga4 = load_dirty_ga4_csv(GA4_CSV)


   # Auto-detect URL Column
   possible_url_cols = ['Page path and screen class', 'Page path', 'Page URL']
   url_col = next((col for col in possible_url_cols if col in df_ga4.columns), None)
   if not url_col:
       url_col = df_ga4.columns[0]  # Fallback
       print(f"[!] Warning: Standard URL column not found. Defaulting to column: '{url_col}'")


   # Auto-detect Sessions Column
   possible_session_cols = ['Sessions', 'Total users', 'Views']
   session_col = next((col for col in possible_session_cols if col in df_ga4.columns), None)
   if not session_col:
       session_col = df_ga4.columns[1]  # Fallback
       print(f"[!] Warning: Standard Sessions column not found. Defaulting to: '{session_col}'")


   # Apply normalization
   df_ga4['normalized_url'] = df_ga4[url_col].apply(normalize_url)
   df_ga4 = df_ga4.dropna(subset=['normalized_url'])


   success_count = 0
   for index, row in df_ga4.iterrows():
       try:
           # Clean numerical data (GA4 sometimes exports "1,500" as a string)
           sessions_raw = str(row[session_col]).replace(',', '')
           sessions_val = int(float(sessions_raw)) if sessions_raw.replace('.', '', 1).isdigit() else 0


           # INSERT OR REPLACE ensures we can run this script multiple times safely
           cursor.execute("""
               INSERT OR REPLACE INTO Pages (url, ga4_sessions)
               VALUES (?, ?)
           """, (row['normalized_url'], sessions_val))
           success_count += 1
       except Exception as e:
           print(f"[-] Failed to insert row {index}: {e}")


   conn.commit()
   conn.close()
   print(f"[+] Step 1 Complete. Inserted {success_count} rows into {DB_NAME}.")




def delete_database_initially():
   conn = sqlite3.connect(DB_NAME)
   cursor = conn.cursor()
   cursor.execute("DROP TABLE IF EXISTS Pages")
   conn.commit()
   conn.close()




if __name__ == "__main__":
   delete_database_initially()
   build_database()






