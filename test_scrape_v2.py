#!/usr/bin/env python3
"""
Тестовый скрипт v2 - проверка на 5 специализациях (все страницы)
"""

import requests
from bs4 import BeautifulSoup
import time
import json

BASE_URL = "https://prodoctorov.ru"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
}

TEST_SPECIALTIES = [
    "/moskva/algolog/",       # Маленькая - ~1 страница
    "/moskva/geriatr/",       # Маленькая - ~1-2 страницы
    "/moskva/gipnolog/",      # Очень маленькая
    "/moskva/dermatolog/",    # Средняя - ~6 страниц
    "/moskva/ginekolog/",     # Большая - ~10 страниц
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
    pages = [int(a.text.strip()) for a in pagination if a.text.strip().isdigit()]
    return max(pages) if pages else 1


def clean_text(text):
    if not text:
        return ""
    return ' '.join(text.split())


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
        name = card.get('data-doctor-name', '')

        link_el = card.select_one('a.b-doctor-card__name-link')
        href = link_el.get('href') if link_el else None

        rating_el = card.select_one('div.b-stars-rate__progress')
        rating = None
        if rating_el:
            style = rating_el.get('style', '')
            if 'width:' in style:
                try:
                    rating = float(style.split('width:')[1].split('em')[0].strip())
                except:
                    pass

        reviews_el = card.select_one('a[href*="#otzivi"]')
        reviews_count = None
        if reviews_el:
            try:
                reviews_count = int(''.join(filter(str.isdigit, reviews_el.text)))
            except:
                pass

        spec_el = card.select_one('div.b-doctor-card__spec')
        specialty = clean_text(spec_el.get_text()) if spec_el else None

        if href and doctor_id:
            doctors.append({
                'id': doctor_id,
                'name': name,
                'url': clean_url(BASE_URL + href),
                'rating': rating,
                'reviews_count': reviews_count,
                'specialty_display': specialty,
            })
    return doctors


def scrape_specialty(specialty_path):
    first_url = BASE_URL + specialty_path
    soup = get_soup(first_url)

    if not soup:
        return []

    last_page = get_last_page(soup)
    all_doctors = parse_doctors(soup)

    print(f"  Страниц: {last_page}")

    for page in range(2, last_page + 1):
        url = f"{first_url}?page={page}"
        soup = get_soup(url)
        doctors = parse_doctors(soup)
        all_doctors.extend(doctors)
        print(f"    Стр. {page}: +{len(doctors)} врачей")
        time.sleep(0.8)

    return all_doctors


def main():
    print("=" * 60)
    print("ТЕСТ v2 - 5 специализаций, ВСЕ страницы")
    print("=" * 60)

    all_doctors = {}

    for specialty in TEST_SPECIALTIES:
        name = specialty.strip('/').split('/')[-1]
        print(f"\n>>> {name}")

        doctors = scrape_specialty(specialty)

        new_count = 0
        for doc in doctors:
            doc_id = doc['id']
            if doc_id not in all_doctors:
                all_doctors[doc_id] = doc
                all_doctors[doc_id]['specialties'] = []
                new_count += 1
            all_doctors[doc_id]['specialties'].append(name)

        print(f"  Итого: {len(doctors)}, новых: {new_count}")
        time.sleep(1)

    print("\n" + "=" * 60)
    print(f"ВСЕГО уникальных врачей: {len(all_doctors)}")
    print("=" * 60)

    # Проверка данных
    print("\nПроверка качества данных:")
    with_rating = sum(1 for d in all_doctors.values() if d.get('rating'))
    with_reviews = sum(1 for d in all_doctors.values() if d.get('reviews_count'))
    with_spec = sum(1 for d in all_doctors.values() if d.get('specialty_display'))

    print(f"  С рейтингом: {with_rating}/{len(all_doctors)}")
    print(f"  С отзывами: {with_reviews}/{len(all_doctors)}")
    print(f"  Со специальностью: {with_spec}/{len(all_doctors)}")

    # Примеры
    print("\nПримеры (первые 3):")
    for i, doc in enumerate(list(all_doctors.values())[:3]):
        print(f"\n{i+1}. {doc['name']}")
        print(f"   URL: {doc['url']}")
        print(f"   Рейтинг: {doc['rating']}, Отзывов: {doc['reviews_count']}")
        print(f"   Спец: {doc['specialty_display']}")
        print(f"   Категории: {doc['specialties']}")

    # Сохранение
    with open('test_v2_doctors.json', 'w', encoding='utf-8') as f:
        json.dump(list(all_doctors.values()), f, ensure_ascii=False, indent=2)

    print(f"\nСохранено в test_v2_doctors.json")


if __name__ == "__main__":
    main()
