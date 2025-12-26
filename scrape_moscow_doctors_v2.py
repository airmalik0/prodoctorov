#!/usr/bin/env python3
"""
Сбор ВСЕХ врачей Москвы с prodoctorov.ru
v2: Исправлена пагинация - используем общее количество врачей из meta description
"""

import requests
from bs4 import BeautifulSoup
import time
import csv
import json
import re
import math
from datetime import datetime

BASE_URL = "https://prodoctorov.ru"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en;q=0.8',
}

DOCTORS_PER_PAGE = 20


def get_soup(url, retries=3):
    """Получает BeautifulSoup объект с повторными попытками"""
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, 'html.parser')
        except Exception as e:
            print(f"  Ошибка (попытка {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(5)
    return None


def get_total_doctors(soup):
    """Получает общее количество врачей из meta description или текста страницы"""
    if not soup:
        return None

    # Метод 1: Meta description
    meta = soup.select_one('meta[name="description"]')
    if meta:
        content = meta.get('content', '')
        # Ищем паттерны типа "6537 врачей" или "6537 гинекологов"
        match = re.search(r'(\d+)\s*(?:врач|доктор|гинеколог|терапевт|специалист)', content, re.IGNORECASE)
        if match:
            return int(match.group(1))

    # Метод 2: H1 или заголовок
    h1 = soup.select_one('h1')
    if h1:
        text = h1.get_text()
        match = re.search(r'(\d+)', text)
        if match:
            return int(match.group(1))

    # Метод 3: Поиск по всему тексту
    text = soup.get_text()
    matches = re.findall(r'(\d{3,6})\s*(?:врач|доктор)', text, re.IGNORECASE)
    if matches:
        return int(matches[0])

    return None


def get_last_page(soup, total_doctors=None):
    """Определяет номер последней страницы"""
    if total_doctors:
        return math.ceil(total_doctors / DOCTORS_PER_PAGE)

    # Fallback: старый метод через пагинатор
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


def parse_doctors_from_page(soup):
    """Извлекает данные врачей со страницы"""
    doctors = []
    if not soup:
        return doctors

    for card in soup.select('div.b-doctor-card[data-doctor-id]'):
        doctor_id = card.get('data-doctor-id')
        name = card.get('data-doctor-name', '')

        link_el = card.select_one('a.b-doctor-card__name-link')
        href = link_el.get('href') if link_el else None

        # Рейтинг
        rating_el = card.select_one('div.b-stars-rate__progress')
        rating = None
        if rating_el:
            style = rating_el.get('style', '')
            if 'width:' in style:
                try:
                    rating = float(style.split('width:')[1].split('em')[0].strip())
                except:
                    pass

        # Количество отзывов
        reviews_el = card.select_one('a[href*="#otzivi"]')
        reviews_count = None
        if reviews_el:
            text = reviews_el.get_text(strip=True)
            try:
                reviews_count = int(''.join(filter(str.isdigit, text)))
            except:
                pass

        # Специальность
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


def scrape_all_doctors():
    """Собирает ВСЕХ врачей с /moskva/vrach/"""
    print("=" * 60)
    print("Сбор ВСЕХ врачей Москвы")
    print("=" * 60)

    first_url = BASE_URL + "/moskva/vrach/"
    soup = get_soup(first_url)

    if not soup:
        print("Ошибка загрузки первой страницы!")
        return {}

    total_doctors = get_total_doctors(soup)
    if total_doctors:
        print(f"Всего врачей на сайте: {total_doctors}")
        last_page = get_last_page(soup, total_doctors)
    else:
        print("Не удалось определить общее количество врачей")
        # Пробуем определить эмпирически
        last_page = 7000  # Примерно 124907 / 20

    print(f"Страниц для обработки: {last_page}")

    all_doctors = {}  # id -> doctor data

    # Первая страница уже загружена
    doctors = parse_doctors_from_page(soup)
    for doc in doctors:
        all_doctors[doc['id']] = doc
    print(f"Страница 1: {len(doctors)} врачей, всего: {len(all_doctors)}")

    # Остальные страницы
    for page in range(2, last_page + 1):
        url = f"{first_url}?page={page}"
        soup = get_soup(url)

        if not soup:
            print(f"  Страница {page}: ошибка загрузки, пропуск")
            continue

        doctors = parse_doctors_from_page(soup)

        if not doctors:
            print(f"  Страница {page}: пустая, возможно достигнут конец")
            # Проверяем ещё 3 страницы на случай временной ошибки
            empty_count = 1
            for check_page in range(page + 1, page + 4):
                check_url = f"{first_url}?page={check_page}"
                check_soup = get_soup(check_url)
                if check_soup and parse_doctors_from_page(check_soup):
                    empty_count = 0
                    break
                empty_count += 1
                time.sleep(0.5)

            if empty_count >= 3:
                print(f"  Конец данных на странице {page}")
                break
            continue

        for doc in doctors:
            if doc['id'] not in all_doctors:
                all_doctors[doc['id']] = doc

        if page % 100 == 0:
            print(f"Страница {page}/{last_page}: всего уникальных {len(all_doctors)}")
            # Сохраняем checkpoint
            with open('moscow_all_doctors_checkpoint.json', 'w', encoding='utf-8') as f:
                json.dump({
                    'doctors': list(all_doctors.values()),
                    'last_page': page
                }, f, ensure_ascii=False)

        time.sleep(0.8)  # Пауза между запросами

    return all_doctors


def main():
    start_time = datetime.now()

    all_doctors = scrape_all_doctors()

    # Сохранение результатов
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # CSV
    csv_file = f"moscow_all_doctors_{timestamp}.csv"
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'url', 'rating', 'reviews_count', 'specialty_display'])
        for doc in all_doctors.values():
            writer.writerow([
                doc['id'],
                doc['name'],
                doc['url'],
                doc.get('rating', ''),
                doc.get('reviews_count', ''),
                doc.get('specialty_display', ''),
            ])

    # JSON
    json_file = f"moscow_all_doctors_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(list(all_doctors.values()), f, ensure_ascii=False, indent=2)

    end_time = datetime.now()
    duration = end_time - start_time

    print("\n" + "=" * 60)
    print("ГОТОВО!")
    print(f"Уникальных врачей: {len(all_doctors)}")
    print(f"Время выполнения: {duration}")
    print(f"CSV: {csv_file}")
    print(f"JSON: {json_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
