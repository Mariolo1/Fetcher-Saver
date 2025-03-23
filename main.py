
from reading_pages import JustJoinITScraper
from SQL_database import JustJoinITDatabase

def main():
    print("🔄 Rozpoczynam pobieranie ofert z JustJoinIT...")
    scraper = JustJoinITScraper(pages_to_fetch=10)
    scraper.fetch_many_pages()
    
    print("💾 Rozpoczynam zapis ofert do bazy danych...")
    db_handler = JustJoinITDatabase()
    db_handler.process_files()
    db_handler.fetch_today_offers()
    db_handler.close_connection()
    
    print("✅ Proces zakończony pomyślnie!")

if __name__ == "__main__":
    main()

