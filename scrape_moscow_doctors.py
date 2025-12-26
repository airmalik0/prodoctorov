#!/usr/bin/env python3
"""
Сбор всех врачей Москвы с prodoctorov.ru
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

# Все специализации Москвы (без /moskva/vrach/ - там пагинация не работает)
SPECIALTIES = [
    "/moskva/abdominalniy-hirurg/",
    "/moskva/akusher/",
    "/moskva/akusherka/",
    "/moskva/algolog/",
    "/moskva/allergolog/",
    "/moskva/androlog/",
    "/moskva/anesteziolog-reanimatolog/",
    "/moskva/arrhythmolog/",
    "/moskva/artrolog/",
    "/moskva/afaziolog/",
    "/moskva/bariatricheskiy-hirurg/",
    "/moskva/venerolog/",
    "/moskva/vertebrolog/",
    "/moskva/kosmetolog/",
    "/moskva/vrach-lechebnoy-fizkultury/",
    "/moskva/vrach-obshyay-praktiki/",
    "/moskva/podolog/",
    "/moskva/vrach-skoroy-pomoshi/",
    "/moskva/ultrazvukovoy-diagnost/",
    "/moskva/vrach-frm/",
    "/moskva/vrach-efferentnoy-terapii/",
    "/moskva/gastroenterolog/",
    "/moskva/gematolog/",
    "/moskva/gemostaziolog/",
    "/moskva/genetik/",
    "/moskva/gepatolog/",
    "/moskva/geriatr/",
    "/moskva/ginekolog/",
    "/moskva/ginekilog-hirurg/",
    "/moskva/ginekolog-endokrinolog/",
    "/moskva/gipnolog/",
    "/moskva/girudoterapevt/",
    "/moskva/gnatolog/",
    "/moskva/gnoynyy-hirurg/",
    "/moskva/gomeopat/",
    "/moskva/dermatovenerolog/",
    "/moskva/dermatolog/",
    "/moskva/detskiy-allergolog/",
    "/moskva/detskiy-androlog/",
    "/moskva/detskiy-anesteziolog-reanimatolog/",
    "/moskva/detskiy-aritmolog/",
    "/moskva/detskiy-venerolog/",
    "/moskva/detskiy-vertebrolog/",
    "/moskva/detskiy-vrach-kosmetolog/",
    "/moskva/detskiy-vrach-lfk/",
    "/moskva/detskiy-vrach-uzi/",
    "/moskva/detskiy-gastroenterolog/",
    "/moskva/detskiy-gematolog/",
    "/moskva/detskiy-genetik/",
    "/moskva/detskiy-gepatolog/",
    "/moskva/detskiy-ginekolog/",
    "/moskva/detskiy-ginekolog-endokrinolog/",
    "/moskva/detskiy-gnatolog/",
    "/moskva/detskiy-gomeopat/",
    "/moskva/detskiy-dermatolog/",
    "/moskva/detskiy-dietolog/",
    "/moskva/detskiy-immunolog/",
    "/moskva/detskiy-instruktor-lfk/",
    "/moskva/detskiy-infekcionist/",
    "/moskva/detskiy-kardiolog/",
    "/moskva/detskiy-kineziolog/",
    "/moskva/detskiy-otorinolaringolog/",
    "/moskva/detskiy-lor-hirurg/",
    "/moskva/detskiy-mammolog/",
    "/moskva/detskiy-manualnyy-terapevt/",
    "/moskva/detskiy-massagist/",
    "/moskva/detskiy-mikolog/",
    "/moskva/detskiy-narkolog/",
    "/moskva/detskiy-nevrolog/",
    "/moskva/detskiy-nyayropsiholog/",
    "/moskva/detskiy-nyayrohirurg/",
    "/moskva/detskiy-nefrolog/",
    "/moskva/detskiy-nutriciolog/",
    "/moskva/detskiy-onkolog/",
    "/moskva/detskiy-onkolog-dermatolog/",
    "/moskva/detskiy-ortodont/",
    "/moskva/detskiy-ortoped-travmatolog/",
    "/moskva/detskiy-osteopat/",
    "/moskva/detskiy-otonevrolog/",
    "/moskva/detskiy-oftalmolog/",
    "/moskva/detskiy-parodontolog/",
    "/moskva/detskiy-plasticheskiy-hirurg/",
    "/moskva/detskiy-podolog/",
    "/moskva/detskiy-proktolog/",
    "/moskva/detskiy-psihiatr/",
    "/moskva/detskiy-psiholog/",
    "/moskva/detskiy-psihoterapevt/",
    "/moskva/detskiy-pulmonolog/",
    "/moskva/detskiy-reabilitolog/",
    "/moskva/detskiy-revmatolog/",
    "/moskva/detskiy-rentgenolog/",
    "/moskva/detskiy-refleksoterapevt/",
    "/moskva/detskiy-seksolog/",
    "/moskva/detskiy-somnolog/",
    "/moskva/detskiy-sosudistyy-hirurg/",
    "/moskva/detskiy-sportivnyy-vrach/",
    "/moskva/detskiy-stomatolog/",
    "/moskva/detskiy-stomatolog-gigienist/",
    "/moskva/detskiy-stomatolog-ortoped/",
    "/moskva/detskiy-stomatolog-hirurg/",
    "/moskva/detskiy-surdolog/",
    "/moskva/detskiy-torakalnyy-hirurg/",
    "/moskva/detskiy-travmatolog/",
    "/moskva/detskiy-triholog/",
    "/moskva/detskiy-urolog/",
    "/moskva/detskiy-fizioterapevt/",
    "/moskva/detskiy-flebolog/",
    "/moskva/detskiy-foniatr/",
    "/moskva/detskiy-ftiziatr/",
    "/moskva/detskiy-hirurg/",
    "/moskva/detskiy-hirurg-ortoped/",
    "/moskva/detskiy-hirurg-travmatolog/",
    "/moskva/detskiy-chelyustno-licevoy-hirurg/",
    "/moskva/detskiy-endokrinolog/",
    "/moskva/detskiy-epileptolog/",
    "/moskva/defektolog/",
    "/moskva/diabetolog/",
    "/moskva/dietolog/",
    "/moskva/immunolog/",
    "/moskva/instruktor-lfk/",
    "/moskva/infekcionist/",
    "/moskva/kardiolog/",
    "/moskva/kardiohirurg/",
    "/moskva/kinesiolog/",
    "/moskva/kistevoy-hirurg/",
    "/moskva/klinicheskiy-psiholog/",
    "/moskva/klinicheskiy-pharmakolog/",
    "/moskva/kosmetolog-estetist/",
    "/moskva/lazernyy-hirurg/",
    "/moskva/limfolog/",
    "/moskva/logoped/",
    "/moskva/logoped-dlya-vzroslyh/",
    "/moskva/otorinolaringolog/",
    "/moskva/lor-hirurg/",
    "/moskva/maloinvazivnyy-hirurg/",
    "/moskva/mammolog/",
    "/moskva/manualnyy-terapevt/",
    "/moskva/massazhist/",
    "/moskva/medsestra/",
    "/moskva/mikolog/",
    "/moskva/narkolog/",
    "/moskva/nevrolog/",
    "/moskva/neuropsiholog/",
    "/moskva/neyrourolog/",
    "/moskva/neyrofiziolog/",
    "/moskva/nyayrohirurg/",
    "/moskva/neonatolog/",
    "/moskva/nefrolog/",
    "/moskva/nutriciolog/",
    "/moskva/ozhogovyy-hirurg/",
    "/moskva/onkolog/",
    "/moskva/onkolog-gematolog/",
    "/moskva/onkolog-ginekolog/",
    "/moskva/onkolog-dermatolog/",
    "/moskva/onkolog-mammolog/",
    "/moskva/onkolog-proktolog/",
    "/moskva/onkolog-urolog/",
    "/moskva/optometrist/",
    "/moskva/ortoped/",
    "/moskva/osteopat/",
    "/moskva/otonevrolog/",
    "/moskva/oftalmolog/",
    "/moskva/hirurg-oftalmolog/",
    "/moskva/parazitolog/",
    "/moskva/paradontolog/",
    "/moskva/pediatr/",
    "/moskva/perinatolog/",
    "/moskva/plasticheskiy-hirurg/",
    "/moskva/podolog-estetist/",
    "/moskva/proktolog/",
    "/moskva/profpatolog/",
    "/moskva/psihiatr/",
    "/moskva/psihoanalitik/",
    "/moskva/psiholog/",
    "/moskva/psihoterapevt/",
    "/moskva/pulmonolog/",
    "/moskva/radiolog/",
    "/moskva/radioterapevt/",
    "/moskva/reabilitolog/",
    "/moskva/revmatolog/",
    "/moskva/rentgenolog/",
    "/moskva/reproduktolog/",
    "/moskva/refleksoterapevt/",
    "/moskva/seksolog/",
    "/moskva/semyaynyy-psiholog/",
    "/moskva/somnolog/",
    "/moskva/sosudistyj-hirurg/",
    "/moskva/specialist-po-grudnomu-vskarmlivaniy/",
    "/moskva/sportivnyy-vrach/",
    "/moskva/stomatolog/",
    "/moskva/stomatolog-gigienist/",
    "/moskva/stomatolog-implantolog/",
    "/moskva/ortodont/",
    "/moskva/stomatolog-ortoped/",
    "/moskva/stomatolog-hirurg/",
    "/moskva/stomatolog-endodontist/",
    "/moskva/sudebno-medicinskiy-ekspert/",
    "/moskva/surdolog/",
    "/moskva/terapevt/",
    "/moskva/toksikolog/",
    "/moskva/torakalnyy-onkolog/",
    "/moskva/torakalnyy-hirurg/",
    "/moskva/travmatolog/",
    "/moskva/transfuziolog/",
    "/moskva/triholog/",
    "/moskva/uroginekolog/",
    "/moskva/urolog/",
    "/moskva/feldsher/",
    "/moskva/fizioterapevt/",
    "/moskva/fitoterapevt/",
    "/moskva/flebolog/",
    "/moskva/foniatr/",
    "/moskva/ftiziatr/",
    "/moskva/funkcionalnyy-diagnost/",
    "/moskva/himioterapevt/",
    "/moskva/hirurg/",
    "/moskva/hirurg-ortoped/",
    "/moskva/hirurg-travmatolog/",
    "/moskva/hirurg-endokrinolog/",
    "/moskva/chelyustno-licevoy-hirurg/",
    "/moskva/embriolog/",
    "/moskva/endokrinolog/",
    "/moskva/endoskopist/",
    "/moskva/epileptolog/",
    "/moskva/ergoterapevt/",
]


def get_soup(url, retries=3):
    """Получает BeautifulSoup объект с повторными попытками"""
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


def get_total_from_meta(soup):
    """Получает общее количество врачей из meta description"""
    if not soup:
        return None

    meta = soup.select_one('meta[name="description"]')
    if meta:
        content = meta.get('content', '')
        # Ищем паттерн "N врачей" или "N специальность"
        match = re.search(r'(\d+)\s*(?:врач|доктор|гинеколог|терапевт|педиатр|хирург|специалист)', content, re.IGNORECASE)
        if match:
            return int(match.group(1))
    return None


def get_last_page(soup):
    """Определяет номер последней страницы"""
    if not soup:
        return 1

    # Метод 1: Из meta description (более точный)
    total = get_total_from_meta(soup)
    if total:
        return math.ceil(total / 20)  # 20 врачей на страницу

    # Метод 2: Fallback на пагинатор (менее точный)
    pagination = soup.select('ul.b-pagination-vuetify-imitation a')
    pages = []
    for a in pagination:
        text = a.get_text(strip=True)
        if text.isdigit():
            pages.append(int(text))

    return max(pages) if pages else 1


def clean_text(text):
    """Очищает текст от лишних пробелов и переносов"""
    if not text:
        return ""
    return ' '.join(text.split())


def clean_url(url):
    """Убирает #filter=default и прочие якоря из URL"""
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


def scrape_specialty(specialty_path):
    """Собирает всех врачей по одной специальности"""
    first_url = BASE_URL + specialty_path
    soup = get_soup(first_url)

    if not soup:
        print(f"  Не удалось загрузить {specialty_path}")
        return []

    total_from_meta = get_total_from_meta(soup)
    last_page = get_last_page(soup)
    all_doctors = parse_doctors_from_page(soup)

    if total_from_meta:
        print(f"  Всего: {total_from_meta} врачей, страниц: {last_page}")
    else:
        print(f"  Страниц: {last_page} (fallback), врачей на 1-й: {len(all_doctors)}")

    for page in range(2, last_page + 1):
        url = f"{first_url}?page={page}"
        soup = get_soup(url)
        doctors = parse_doctors_from_page(soup)
        all_doctors.extend(doctors)

        if page % 5 == 0 or page == last_page:
            print(f"    Страница {page}/{last_page}, всего: {len(all_doctors)}")

        time.sleep(1.0)

    return all_doctors


def main():
    print("=" * 60)
    print("Сбор врачей Москвы с prodoctorov.ru")
    print(f"Специализаций: {len(SPECIALTIES)}")
    print("=" * 60)

    all_doctors = {}  # id -> doctor data (для дедупликации)
    stats = []

    for i, specialty in enumerate(SPECIALTIES, 1):
        specialty_name = specialty.strip('/').split('/')[-1]
        print(f"\n[{i}/{len(SPECIALTIES)}] {specialty_name}")

        doctors = scrape_specialty(specialty)

        # Добавляем с дедупликацией
        new_count = 0
        for doc in doctors:
            doc_id = doc['id']
            if doc_id not in all_doctors:
                all_doctors[doc_id] = doc
                all_doctors[doc_id]['specialties'] = []
                new_count += 1
            # Обновляем рейтинг и отзывы если появились новые данные
            if doc.get('rating') and not all_doctors[doc_id].get('rating'):
                all_doctors[doc_id]['rating'] = doc['rating']
            if doc.get('reviews_count') and not all_doctors[doc_id].get('reviews_count'):
                all_doctors[doc_id]['reviews_count'] = doc['reviews_count']
            all_doctors[doc_id]['specialties'].append(specialty_name)

        stats.append({
            'specialty': specialty_name,
            'total': len(doctors),
            'new': new_count
        })

        print(f"  Найдено: {len(doctors)}, новых: {new_count}, всего уникальных: {len(all_doctors)}")

        # Промежуточное сохранение каждые 20 специализаций
        if i % 20 == 0:
            with open('moscow_doctors_checkpoint.json', 'w', encoding='utf-8') as f:
                json.dump({
                    'doctors': list(all_doctors.values()),
                    'stats': stats,
                    'last_index': i
                }, f, ensure_ascii=False)
            print(f"  [Checkpoint сохранён: {len(all_doctors)} врачей]")

        time.sleep(1.5)

    # Сохранение результатов
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # CSV с основными данными
    csv_file = f"moscow_doctors_{timestamp}.csv"
    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'url', 'rating', 'reviews_count', 'specialty_display', 'specialties'])
        for doc in all_doctors.values():
            writer.writerow([
                doc['id'],
                doc['name'],
                doc['url'],
                doc.get('rating', ''),
                doc.get('reviews_count', ''),
                doc.get('specialty_display', ''),
                '; '.join(doc['specialties'])
            ])

    # JSON с полными данными
    json_file = f"moscow_doctors_{timestamp}.json"
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(list(all_doctors.values()), f, ensure_ascii=False, indent=2)

    # Статистика
    stats_file = f"moscow_doctors_stats_{timestamp}.csv"
    with open(stats_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['specialty', 'total', 'new'])
        writer.writeheader()
        writer.writerows(stats)

    print("\n" + "=" * 60)
    print("ГОТОВО!")
    print(f"Уникальных врачей: {len(all_doctors)}")
    print(f"Сохранено в: {csv_file}")
    print(f"JSON: {json_file}")
    print(f"Статистика: {stats_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
