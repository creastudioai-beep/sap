import json
import os
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
import requests

CHANNEL_URL = "https://t.me/s/sochiautoparts"

# ============================================
# НАСТРОЙКИ ЛИМИТОВ
# ============================================
PARSE_LIMIT = 1000   # Сколько постов парсить за один запуск (для скорости)
CACHE_LIMIT = 5555   # Сколько постов хранить в кеше (история)
# ============================================

CACHE_FILE = "data/cached_posts.json"

def parse_telegram_channel():
    """Парсит последние посты из публичного Telegram канала."""
    all_posts = []
    next_url = CHANNEL_URL
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    print(f"Начинаем парсинг канала: {CHANNEL_URL}")
    print(f"Лимит парсинга: {PARSE_LIMIT} постов")

    try:
        session = requests.Session()

        while len(all_posts) < PARSE_LIMIT and next_url:
            print(f"Загружаем: {next_url}")
            response = session.get(next_url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            message_wrappers = soup.find_all('div', class_='tgme_widget_message_wrap')
            print(f"Найдено сообщений на странице: {len(message_wrappers)}")

            for wrap in message_wrappers:
                if len(all_posts) >= PARSE_LIMIT:
                    break

                post = {
                    'id': None,
                    'date': '',
                    'text': '',
                    'photo_url': '',
                    'video_url': '',
                    'links': [],
                    'parsed_at': datetime.now().isoformat()
                }

                message_div = wrap.find('div', class_='tgme_widget_message')
                if message_div and 'data-post' in message_div.attrs:
                    post['id'] = message_div['data-post']

                date_elem = wrap.find('a', class_='tgme_widget_message_date')
                if date_elem and date_elem.time:
                    post['date'] = date_elem.time['datetime']
                elif date_elem:
                    post['date'] = date_elem.text.strip()

                text_elem = wrap.find('div', class_='tgme_widget_message_text')
                if text_elem:
                    for br in text_elem.find_all('br'):
                        br.replace_with('\n')
                    post['text'] = text_elem.get_text().strip()

                    text_links = text_elem.find_all('a', href=True)
                    for link in text_links:
                        href = link.get('href', '')
                        if href and not href.startswith('https://t.me/') and href not in post['links']:
                            post['links'].append(href)

                photo_wrap = wrap.find('a', class_='tgme_widget_message_photo_wrap')
                if photo_wrap:
                    style = photo_wrap.get('style', '')
                    if style:
                        match = re.search(r"url('(.*?)')", style)
                        if match:
                            post['photo_url'] = match.group(1)

                if not post['photo_url']:
                    img = wrap.find('img', class_='tgme_widget_message_photo')
                    if img and img.get('src'):
                        post['photo_url'] = img['src']

                video_elem = wrap.find('video', class_='tgme_widget_message_video')
                if video_elem:
                    source = video_elem.find('source')
                    if source and source.get('src'):
                        post['video_url'] = source['src']
                    elif video_elem.get('src'):
                        post['video_url'] = video_elem['src']

                other_links = wrap.find_all('a', class_='tgme_widget_message_link')
                for link in other_links:
                    href = link.get('href', '')
                    if href and not href.startswith('https://t.me/') and href not in post['links']:
                        post['links'].append(href)

                if post['text'] or post['photo_url'] or post['video_url'] or post['links']:
                    all_posts.append(post)
                    print(f"  Добавлен пост: {post['date']}")

            load_more = soup.find('a', class_='tme_messages_more')
            if load_more and load_more.get('href'):
                next_url = f"https://t.me{load_more['href']}"
                print(f"Найдена ссылка на следующие сообщения")
            else:
                next_url = None
                print("Больше сообщений не найдено")

            if next_url:
                time.sleep(1)

    except requests.RequestException as e:
        print(f"Ошибка при загрузке страницы: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")

    print(f"Всего собрано постов: {len(all_posts)}")
    return all_posts


def update_cache(new_posts):
    """
    Обновляет кеш, добавляя ТОЛЬКО НОВЫЕ посты.
    Хранит до CACHE_LIMIT постов в файле.
    """
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)

    cached_posts = []
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cached_posts = json.load(f)
            print(f"Загружен существующий кеш: {len(cached_posts)} постов")
        except Exception as e:
            print(f"Ошибка при загрузке кеша: {e}")
            cached_posts = []

    existing_ids = {p.get('id') for p in cached_posts if p.get('id')}
    print(f"Уникальных постов в кеше: {len(existing_ids)}")

    truly_new_posts = []
    for post in new_posts:
        post_id = post.get('id')
        if post_id and post_id not in existing_ids:
            truly_new_posts.append(post)
            existing_ids.add(post_id)
            print(f"  Новый пост добавлен: {post_id}")

    print(f"Найдено новых постов для добавления: {len(truly_new_posts)}")

    if truly_new_posts:
        cached_posts = truly_new_posts + cached_posts
        print(f"Новые посты добавлены в кеш")
    else:
        print("Новых постов нет, кеш не изменён")

    # Удаляем дубликаты
    unique_posts = []
    seen_ids = set()
    for post in cached_posts:
        post_id = post.get('id')
        if post_id and post_id not in seen_ids:
            seen_ids.add(post_id)
            unique_posts.append(post)

    # Обрезаем до CACHE_LIMIT
    final_posts = unique_posts[:CACHE_LIMIT]
    
    if len(unique_posts) > CACHE_LIMIT:
        removed = len(unique_posts) - CACHE_LIMIT
        print(f"Кеш обрезан до {CACHE_LIMIT} постов (удалено {removed} старых)")

    if truly_new_posts or len(final_posts) != len(cached_posts):
        try:
            with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(final_posts, f, ensure_ascii=False, indent=2, default=str)
            print(f"Кеш обновлён и сохранён. Всего постов: {len(final_posts)}")
        except Exception as e:
            print(f"Ошибка при сохранении кеша: {e}")
    else:
        print("Кеш не изменён, файл не перезаписывается")

    return final_posts, len(truly_new_posts)


def main():
    """Основная функция парсера."""
    print("=" * 50)
    print("Telegram Channel Parser")
    print(f"Канал: {CHANNEL_URL}")
    print(f"Время запуска: {datetime.now().isoformat()}")
    print(f"Парсинг: до {PARSE_LIMIT} постов")
    print(f"Кеш: до {CACHE_LIMIT} постов")
    print("=" * 50)

    posts = parse_telegram_channel()

    if posts:
        cached, new_count = update_cache(posts)

        print("\n" + "=" * 50)
        print("СТАТИСТИКА:")
        print(f"Получено постов при парсинге: {len(posts)}")
        print(f"Добавлено новых постов: {new_count}")
        print(f"Всего в кеше: {len(cached)}")

        posts_with_photos = sum(1 for p in posts if p.get('photo_url'))
        posts_with_videos = sum(1 for p in posts if p.get('video_url'))
        posts_with_links = sum(1 for p in posts if p.get('links'))

        print(f"Постов с фото: {posts_with_photos}")
        print(f"Постов с видео: {posts_with_videos}")
        print(f"Постов со ссылками: {posts_with_links}")
        print("=" * 50)

        if cached:
            latest_file = "data/latest_posts.json"
            try:
                with open(latest_file, 'w', encoding='utf-8') as f:
                    json.dump(cached[:10], f, ensure_ascii=False, indent=2, default=str)
                print(f"Создан файл с последними 10 постами: {latest_file}")
            except Exception as e:
                print(f"Не удалось создать файл с последними постами: {e}")
    else:
        print("Не удалось получить посты из канала.")

    print("Парсинг завершён.")


if __name__ == "__main__":
    main()
