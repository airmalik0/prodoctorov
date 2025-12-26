#!/usr/bin/env python3
"""
Тест v3 - проверка на первых 100 страницах /moskva/vrach/
"""

import requests
from bs4 import BeautifulSoup
import time
import json

BASE_URL = "https://prodoctorov.ru"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}


def get_soup(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, 'html.parser')
        except Exception as e:
            print(f"  Ошибка: {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return None


def clean_url(url):
    if '#' in url:
        url = url.split('#')[0]
    return url


def parse_doctors(soup):
    doctors = []
    if not soup:
        return doctors

    for card in soup.select('div.b-doctor-card[data-doctor-id]'):
        doctor_id = card.get('data-doctor-id')
        link_el = card.select_one('a.b-doctor-card__name-link')
        href = link_el.get('href') if link_el else None

        if href and doctor_id:
            doctors.append({
                'id': doctor_id,
                'name': card.get('data-doctor-name', ''),
                'url': clean_url(BASE_URL + href),
            })
    return doctors


def main():
    print("Тест: первые 100 страниц /moskva/vrach/")
    print("=" * 50)

    all_doctors = {}
    base_url = BASE_URL + "/moskva/vrach/"

    for page in range(1, 101):
        url = f"{base_url}?page={page}" if page > 1 else base_url
        soup = get_soup(url)
        doctors = parse_doctors(soup)

        for doc in doctors:
            all_doctors[doc['id']] = doc

        if page % 10 == 0:
            print(f"Страница {page}: {len(doctors)} врачей, всего уникальных: {len(all_doctors)}")

        time.sleep(0.5)

    print()
    print("=" * 50)
    print(f"Собрано за 100 страниц: {len(all_doctors)} уникальных врачей")
    print(f"Ожидаемо при 20 врачах/страницу: ~2000")

    # Проверка: нет ли дубликатов на разных страницах
    print()
    print("Примеры (первые 5):")
    for doc in list(all_doctors.values())[:5]:
        print(f"  {doc['id']}: {doc['name']} - {doc['url']}")

    with open('test_v3_100pages.json', 'w', encoding='utf-8') as f:
        json.dump(list(all_doctors.values()), f, ensure_ascii=False, indent=2)

    print(f"\nСохранено в test_v3_100pages.json")


if __name__ == "__main__":
    main()
