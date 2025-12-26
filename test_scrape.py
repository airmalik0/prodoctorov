#!/usr/bin/env python3
"""
Тестовый скрипт - проверка на 3 специализациях
"""

import requests
from bs4 import BeautifulSoup
import time
import json

BASE_URL = "https://prodoctorov.ru"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
}

# Тестируем на 3 специализациях
TEST_SPECIALTIES = [
    "/moskva/ginekolog/",
    "/moskva/terapevt/",
    "/moskva/dermatolog/",
]


def get_soup(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, 'html.parser')
        except Exception as e:
            print(f"  Ошибка (попытка {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(3)
    return None


def get_last_page(soup):
    if not soup:
        return 1
    pagination = soup.select('ul.b-pagination-vuetify-imitation a')
    pages = []
    for a in pagination:
        text = a.get_text(strip=True)
        if text.isdigit():
            pages.append(int(text))
    return max(pages) if pages else 1


def parse_doctors_from_page(soup):
    doctors = []
    if not soup:
        return doctors

    for card in soup.select('div.b-doctor-card[data-doctor-id]'):
        doctor_id = card.get('data-doctor-id')
        name = card.get('data-doctor-name', '')

        link_el = card.select_one('a.b-doctor-card__name-link')
        href = link_el.get('href') if link_el else None

        # Дополнительные данные
        rating_el = card.select_one('div.b-stars-rate__progress')
        rating = None
        if rating_el:
            style = rating_el.get('style', '')
            if 'width:' in style:
                # width: 6.4400em -> рейтинг примерно 6.44/10
                try:
                    rating = float(style.split('width:')[1].split('em')[0].strip())
                except:
                    pass

        reviews_el = card.select_one('a[href*="#otzivi"]')
        reviews_count = None
        if reviews_el:
            text = reviews_el.get_text(strip=True)
            try:
                reviews_count = int(''.join(filter(str.isdigit, text)))
            except:
                pass

        spec_el = card.select_one('div.b-doctor-card__spec')
        specialty = spec_el.get_text(strip=True) if spec_el else None

        if href and doctor_id:
            doctors.append({
                'id': doctor_id,
                'name': name,
                'url': BASE_URL + href,
                'rating': rating,
                'reviews_count': reviews_count,
                'specialty': specialty,
            })

    return doctors


def test_scrape():
    print("=" * 60)
    print("ТЕСТОВЫЙ ЗАПУСК - 3 специализации, первые 2 страницы")
    print("=" * 60)

    all_doctors = {}

    for specialty in TEST_SPECIALTIES:
        specialty_name = specialty.strip('/').split('/')[-1]
        print(f"\n>>> {specialty_name}")

        first_url = BASE_URL + specialty
        soup = get_soup(first_url)

        if not soup:
            print("  Ошибка загрузки!")
            continue

        last_page = get_last_page(soup)
        print(f"  Всего страниц: {last_page}")

        # Парсим только первые 2 страницы для теста
        pages_to_parse = min(2, last_page)

        for page in range(1, pages_to_parse + 1):
            if page == 1:
                page_soup = soup
            else:
                url = f"{first_url}?page={page}"
                page_soup = get_soup(url)
                time.sleep(1)

            doctors = parse_doctors_from_page(page_soup)
            print(f"  Страница {page}: {len(doctors)} врачей")

            for doc in doctors:
                doc_id = doc['id']
                if doc_id not in all_doctors:
                    all_doctors[doc_id] = doc

    print("\n" + "=" * 60)
    print(f"ИТОГО уникальных врачей: {len(all_doctors)}")
    print("=" * 60)

    # Показываем примеры
    print("\nПримеры данных (первые 5):")
    for i, doc in enumerate(list(all_doctors.values())[:5]):
        print(f"\n{i+1}. {doc['name']}")
        print(f"   ID: {doc['id']}")
        print(f"   URL: {doc['url']}")
        print(f"   Рейтинг: {doc['rating']}")
        print(f"   Отзывов: {doc['reviews_count']}")
        print(f"   Специальность: {doc['specialty']}")

    # Сохраняем тестовые данные
    with open('test_doctors.json', 'w', encoding='utf-8') as f:
        json.dump(list(all_doctors.values()), f, ensure_ascii=False, indent=2)

    print(f"\nТестовые данные сохранены в test_doctors.json")

    return all_doctors


if __name__ == "__main__":
    test_scrape()
