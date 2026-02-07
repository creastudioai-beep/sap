import json
import os
import re
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import urllib.parse

CHANNEL_URL = "https://t.me/s/sochiautoparts"
MAX_POSTS = 200
CACHE_FILE = "data/cached_posts.json"

def parse_telegram_channel():
    """Парсит последние посты из публичного Telegram канала."""
    all_posts = []
    next_url = CHANNEL_URL
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    print(f"Начинаем парсинг канала: {CHANNEL_URL}")
    
    try:
        session = requests.Session()
        
        while len(all_posts) < MAX_POSTS and next_url:
            print(f"Загружаем: {next_url}")
            response = session.get(next_url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Ищем все сообщения
            message_wrappers = soup.find_all('div', class_='tgme_widget_message_wrap')
            print(f"Найдено сообщений на странице: {len(message_wrappers)}")
            
            for wrap in message_wrappers:
                if len(all_posts) >= MAX_POSTS:
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

                # Извлекаем ID сообщения
                message_div = wrap.find('div', class_='tgme_widget_message')
                if message_div and 'data-post' in message_div.attrs:
                    post['id'] = message_div['data-post']

                # Извлекаем дату
                date_elem = wrap.find('a', class_='tgme_widget_message_date')
                if date_elem and date_elem.time:
                    post['date'] = date_elem.time['datetime']
                elif date_elem:
                    post['date'] = date_elem.text.strip()

                # Извлекаем текст
                text_elem = wrap.find('div', class_='tgme_widget_message_text')
                if text_elem:
                    # Удаляем ненужные элементы из текста
                    for br in text_elem.find_all('br'):
                        br.replace_with('\n')
                    post['text'] = text_elem.get_text().strip()
                    
                    # Извлекаем все ссылки из текста
                    text_links = text_elem.find_all('a', href=True)
                    for link in text_links:
                        href = link.get('href', '')
                        if href and not href.startswith('https://t.me/') and href not in post['links']:
                            post['links'].append(href)

                # Извлекаем фото (оригинальные ссылки)
                photo_wrap = wrap.find('a', class_='tgme_widget_message_photo_wrap')
                if photo_wrap:
                    style = photo_wrap.get('style', '')
                    if style:
                        match = re.search(r"url\('(.*?)'\)", style)
                        if match:
                            post['photo_url'] = match.group(1)
                    
                    # Пробуем найти прямую ссылку
                    if not post['photo_url']:
                        img = wrap.find('img', class_='tgme_widget_message_photo')
                        if img and img.get('src'):
                            post['photo_url'] = img['src']

                # Извлекаем видео
                video_elem = wrap.find('video', class_='tgme_widget_message_video')
                if video_elem:
                    source = video_elem.find('source')
                    if source and source.get('src'):
                        post['video_url'] = source['src']
                    elif video_elem.get('src'):
                        post['video_url'] = video_elem['src']

                # Ищем дополнительные ссылки
                other_links = wrap.find_all('a', class_='tgme_widget_message_link')
                for link in other_links:
                    href = link.get('href', '')
                    if href and not href.startswith('https://t.me/') and href not in post['links']:
                        post['links'].append(href)

                # Добавляем пост только если есть хоть какие-то данные
                if post['text'] or post['photo_url'] or post['video_url'] or post['links']:
                    all_posts.append(post)
                    print(f"  Добавлен пост: {post['date']}")

            # Ищем кнопку "Загрузить предыдущие"
            load_more = soup.find('a', class_='tme_messages_more')
            if load_more and load_more.get('href'):
                next_url = f"https://t.me{load_more['href']}"
                print(f"Найдена ссылка на следующие сообщения")
            else:
                next_url = None
                print("Больше сообщений не найдено")

            # Небольшая задержка между запросами
            if next_url:
                time.sleep(1)

    except requests.RequestException as e:
        print(f"Ошибка при загрузке страницы: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")

    print(f"Всего собрано постов: {len(all_posts)}")
    return all_posts

def update_cache(new_posts):
    """Обновляет кеш, сохраняя только актуальные данные."""
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)

    # Загружаем существующий кеш
    cached_posts = []
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cached_posts = json.load(f)
                print(f"Загружен существующий кеш: {len(cached_posts)} постов")
        except Exception as e:
            print(f"Ошибка при загрузке кеша: {e}")

    # Создаем словарь для быстрого поиска существующих постов по ID
    existing_ids = {p.get('id'): i for i, p in enumerate(cached_posts) if p.get('id')}

    # Объединяем старые и новые посты
    for post in new_posts:
        post_id = post.get('id')
        if post_id and post_id in existing_ids:
            # Обновляем существующий пост
            cached_posts[existing_ids[post_id]] = post
        else:
            # Добавляем новый пост в начало (чтобы самые новые были первыми)
            cached_posts.insert(0, post)

    # Ограничиваем количество постов и сохраняем только уникальные по ID
    unique_posts = []
    seen_ids = set()
    for post in cached_posts:
        post_id = post.get('id')
        if post_id and post_id not in seen_ids:
            seen_ids.add(post_id)
            unique_posts.append(post)
    
    # Берем только MAX_POSTS самых свежих
    final_posts = unique_posts[:MAX_POSTS]
    
    # Сохраняем обновленный кеш
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_posts, f, ensure_ascii=False, indent=2, default=str)
        print(f"Кеш сохранен. Всего постов: {len(final_posts)}")
    except Exception as e:
        print(f"Ошибка при сохранении кеша: {e}")
    
    return final_posts

def main():
    """Основная функция парсера."""
    print("=" * 50)
    print("Telegram Channel Parser")
    print(f"Канал: {CHANNEL_URL}")
    print(f"Время запуска: {datetime.now().isoformat()}")
    print("=" * 50)
    
    # Парсим канал
    posts = parse_telegram_channel()
    
    if posts:
        # Обновляем кеш
        cached = update_cache(posts)
        
        # Выводим статистику
        print("\n" + "=" * 50)
        print("СТАТИСТИКА:")
        print(f"Получено новых постов: {len(posts)}")
        print(f"Всего в кеше: {len(cached)}")
        
        posts_with_photos = sum(1 for p in posts if p.get('photo_url'))
        posts_with_videos = sum(1 for p in posts if p.get('video_url'))
        posts_with_links = sum(1 for p in posts if p.get('links'))
        
        print(f"Постов с фото: {posts_with_photos}")
        print(f"Постов с видео: {posts_with_videos}")
        print(f"Постов со ссылками: {posts_with_links}")
        print("=" * 50)
        
        # Сохраняем также отдельный файл с последними 10 постами для быстрого просмотра
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
    
    print("Парсинг завершен.")

if __name__ == "__main__":
    main()
