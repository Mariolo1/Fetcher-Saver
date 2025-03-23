import requests
import json
import os
from time import sleep
import random

class JustJoinITScraper:
    def __init__(self, per_page=100, retries=3, pages_to_fetch=10):
        self.per_page = per_page
        self.retries = retries
        self.pages_to_fetch = pages_to_fetch
        self.headers = {"Version": "2"}
        self.last_page_file = 'last_page.txt'
    
    def fetch_one_page(self, page):
        print(f"Pobieranie strony {page}...")
        attempt = 0
        while attempt < self.retries:
            try:
                response = requests.get(
                    f'https://api.justjoin.it/v2/user-panel/offers?&page={page}&sortBy=published&orderBy=DESC&perPage={self.per_page}&salaryCurrencies=PLN',
                    headers=self.headers
                )
                
                if response.status_code == 200:
                    with open(rf'../../data/justjoinit_offers_page_{page}.json', 'w') as f:    
                        json.dump(response.json(), f, indent=4)
                    print(f"Strona {page} zapisana pomyślnie.")
                    return True
                else:
                    print(f"Błąd podczas pobierania strony {page}: {response.status_code}")
                    attempt += 1
                    sleep(2)
            except requests.RequestException as e:
                print(f"Wyjątek podczas pobierania strony {page}: {e}")
                attempt += 1
                sleep(2)
        
        print(f"Nie udało się pobrać strony {page} po {self.retries} próbach.")
        return False

    def fetch_many_pages(self):
        last_page = self.get_last_page()
        
        if last_page >= self.pages_to_fetch:
            print("Wszystkie strony zostały już pobrane. Resetuję licznik.")
            last_page = 0
            self.save_last_page(last_page)
        
        for page in range(last_page + 1, self.pages_to_fetch + 1):
            success = self.fetch_one_page(page)
            
            if success:
                self.save_last_page(page)
            else:
                print(f"Nie udało się pobrać strony {page}, przechodzimy do następnej.")
            
            sleep(random.randint(1, 4))
    
    def get_last_page(self):
        if os.path.exists(self.last_page_file):
            with open(self.last_page_file, 'r') as file:
                return int(file.read())
        return 0
    
    def save_last_page(self, page):
        with open(self.last_page_file, 'w') as file:
            file.write(str(page))

if __name__ == "__main__":
    scraper = JustJoinITScraper(pages_to_fetch=10)
    scraper.fetch_many_pages()
