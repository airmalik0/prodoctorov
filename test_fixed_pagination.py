#!/usr/bin/env python3
"""
Тест исправленной пагинации на 3 специализациях
"""

import requests
from bs4 import BeautifulSoup
import time
import json
import re
import math

BASE_URL = "https://prodoctorov.ru"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

TEST_SPECIALTIES = [
    "/moskva/ginekolog/",      # Большая - 6537 врачей
    "/moskva/algolog/",        # Маленькая
    "/moskva/gipnolog/",       # Очень маленькая
]


def get_soup(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, 'html.parser')
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(3)
    return None


def get_total_from_meta(soup):
    if not soup:
        return None
    meta = soup.select_one('meta[name="description"]')
    if meta:
        content = meta.get('content', '')
        match = re.search(r'(\d+)\s*(?:врач|доктор|гинеколог|терапевт|педиатр|хирург|специалист)', content, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def get_last_page(soup):
    if not soup:
        return 1
    total = get_total_from_meta(soup)
    if total:
        return math.ceil(total / 20)
    # Fallback
    pagination = soup.select('ul.b-pagination-vuetify-imitation a')
    pages = [int(a.text.strip()) for a in pagination if a.text.strip().isdigit()]
    return max(pages) if pages else 1


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
    print("=" * 60)
    print("ТЕСТ ИСПРАВЛЕННОЙ ПАГИНАЦИИ")
    print("=" * 60)

    for specialty in TEST_SPECIALTIES:
        name = specialty.strip('/').split('/')[-1]
        print(f"\n>>> {name}")

        url = BASE_URL + specialty
        soup = get_soup(url)

        total = get_total_from_meta(soup)
        last_page = get_last_page(soup)

        print(f"  Из meta: {total} врачей")
        print(f"  Рассчитано страниц: {last_page}")

        # Проверяем первую, среднюю и последнюю страницы
        test_pages = [1, last_page // 2, last_page]
        all_ids = set()

        for page in test_pages:
            page_url = f"{url}?page={page}" if page > 1 else url
            page_soup = get_soup(page_url)
            doctors = parse_doctors(page_soup)
            for d in doctors:
                all_ids.add(d['id'])
            print(f"    Страница {page}: {len(doctors)} врачей")
            time.sleep(0.5)

        print(f"  Уникальных ID на тестовых страницах: {len(all_ids)}")

    print("\n" + "=" * 60)
    print("ТЕСТ ЗАВЕРШЁН")
    print("=" * 60)


if __name__ == "__main__":
    main()
