import sqlite3
import json
import os
from datetime import datetime

class JustJoinITDatabase:
    def __init__(self, folder_path="../../data/", suffix="_Mariolo1"):
        self.today_date = datetime.today().strftime("%Y%m%d")
        self.today_db_date = datetime.today().strftime("%Y-%m-%d")
        self.db_name = f"offers_{self.today_date}{suffix}.db"
        self.folder_path = folder_path
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()
        self.create_table()
    
    def create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS offers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT UNIQUE,
                title TEXT,
                companyName TEXT,
                city TEXT,
                street TEXT,
                latitude REAL,
                longitude REAL,
                workplaceType TEXT,
                experienceLevel TEXT,
                requiredSkills TEXT,
                date_added TEXT
            )
        ''')
        self.conn.commit()
    
    def fetch_json_files(self):
        return sorted([
            f for f in os.listdir(self.folder_path)
            if f.startswith("justjoinit_offers__page_") and f.endswith(".json")
        ])
    
    def process_files(self):
        json_files = self.fetch_json_files()
        added_rows = 0
        skipped_rows = 0
        
        for file_name in json_files:
            file_path = os.path.join(self.folder_path, file_name)
            with open(file_path, "r", encoding="utf-8") as file:
                try:
                    data = json.load(file)
                except json.JSONDecodeError:
                    print(f"Błąd wczytywania JSON: {file_name}")
                    continue
                
                offers = data.get("data", [])
                print(f"Liczba ofert w {file_name}: {len(offers)}")
                
                for offer in offers:
                    slug = offer.get("slug", "N/A")
                    
                    self.cursor.execute("SELECT COUNT(*) FROM offers WHERE slug = ?", (slug,))
                    if self.cursor.fetchone()[0] > 0:
                        print(f"Oferta {slug} już istnieje, pominięto.")
                        skipped_rows += 1
                        continue
                    
                    try:
                        self.cursor.execute('''
                            INSERT INTO offers (slug, title, companyName, city, street, latitude, longitude, workplaceType, experienceLevel, requiredSkills, date_added)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            slug,
                            offer.get("title", "N/A"),
                            offer.get("companyName", "N/A"),
                            offer.get("city", "N/A"),
                            offer.get("street", "-"),
                            float(offer["latitude"]) if offer.get("latitude") else 0.0,
                            float(offer["longitude"]) if offer.get("longitude") else 0.0,
                            offer.get("workplaceType", "N/A"),
                            offer.get("experienceLevel", "N/A"),
                            ", ".join(offer.get("requiredSkills", [])) if offer.get("requiredSkills") else "None",
                            self.today_db_date
                        ))
                        added_rows += 1
                        print(f"Oferta {slug} dodana.")
                    except Exception as e:
                        print(f"Błąd wstawiania danych: {e}")
        
        self.conn.commit()
        print(f"\n📌 Podsumowanie:")
        print(f"✅ Dodano ofert: {added_rows}")
        print(f"❌ Pominięto duplikatów: {skipped_rows}")
        
    def fetch_today_offers(self):
        self.cursor.execute("SELECT * FROM offers WHERE date_added = ?", (self.today_db_date,))
        rows = self.cursor.fetchall()
        print(f"📦 Łącznie w bazie (na dziś): {len(rows)}\n")
        
        if rows:
            for row in rows[:5]:
                print(row)
        else:
            print("Brak ofert dodanych dzisiaj.")
        
    def close_connection(self):
        self.conn.close()
        
if __name__ == "__main__":
    db_handler = JustJoinITDatabase()
    db_handler.process_files()
    db_handler.fetch_today_offers()
    db_handler.close_connection()
