import json
import os
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
import requests

CHANNEL_URL = "https://t.me/s/sochiautoparts"
MAX_POSTS = 200
CACHE_FILE = "data/cached_posts.json"

def parse_telegram_channel():
    """Парсит последние посты из публичного Telegram канала."""
    all_posts = []
    next_url = CHANNEL_URL
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    print(f"Начинаем парсинг канала: {CHANNEL_URL}")
    
    try:
        session = requests.Session()
        
        while len(all_posts) < MAX_POSTS and next_url:
            print(f"Загружаем: {next_url}")
            response = session.get(next_url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            message_wrappers = soup.find_all('div', class_='tgme_widget_message_wrap')
            print(f"Найдено сообщений на странице: {len(message_wrappers)}")
            
            for wrap in message_wrappers:
                if len(all_posts) >= MAX_POSTS:
                    break

                post = {
                    'id': None,
                    'date': '',
                    'text': '',
                    'photo_urls': [],  # Изменено: теперь список
                    'video_urls': [],  # Изменено: теперь список
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
                    for br in text_elem.find_all('br'):
                        br.replace_with('\n')
                    post['text'] = text_elem.get_text().strip()
                    
                    # Извлекаем все ссылки из текста
                    text_links = text_elem.find_all('a', href=True)
                    for link in text_links:
                        href = link.get('href', '')
                        if href and not href.startswith('https://t.me/') and href not in post['links']:
                            post['links'].append(href)

                # ИЗМЕНЕНО: Извлекаем ВСЕ фото
                # 1. Ищем фото в обертках с background-image
                photo_wraps = wrap.find_all('a', class_='tgme_widget_message_photo_wrap')
                for photo_wrap in photo_wraps:
                    style = photo_wrap.get('style', '')
                    if style:
                        # Ищем URL в стиле background-image
                        matches = re.findall(r"url\('(.*?)'\)", style)
                        for match in matches:
                            if match and match not in post['photo_urls']:
                                post['photo_urls'].append(match)
                
                # 2. Ищем обычные теги img
                img_tags = wrap.find_all('img', class_='tgme_widget_message_photo')
                for img in img_tags:
                    src = img.get('src')
                    if src and src not in post['photo_urls']:
                        post['photo_urls'].append(src)
                
                # 3. Ищем фото в других местах (например, превью ссылок)
                other_imgs = wrap.find_all('img', {'src': True})
                for img in other_imgs:
                    src = img.get('src')
                    if (src and 'telegram' in src and 
                        'photo' in src.lower() and 
                        src not in post['photo_urls']):
                        post['photo_urls'].append(src)

                # ИЗМЕНЕНО: Извлекаем ВСЕ видео
                # 1. Ищем теги video
                video_tags = wrap.find_all('video', class_='tgme_widget_message_video')
                for video_elem in video_tags:
                    # Пробуем получить src из тега video
                    video_src = video_elem.get('src')
                    if video_src and video_src not in post['video_urls']:
                        post['video_urls'].append(video_src)
                    
                    # Ищем source внутри video
                    sources = video_elem.find_all('source')
                    for source in sources:
                        src = source.get('src')
                        if src and src not in post['video_urls']:
                            post['video_urls'].append(src)
                
                # 2. Ищем ссылки на видео в других местах
                # Telegram может использовать data-video атрибуты
                video_links = wrap.find_all('a', {'href': True})
                for link in video_links:
                    href = link.get('href', '')
                    if (href and ('video' in href.lower() or 'mp4' in href.lower() or 'mov' in href.lower()) 
                        and 'telegram' in href and href not in post['video_urls']):
                        post['video_urls'].append(href)

                # Ищем дополнительные ссылки (не телеграм)
                other_links = wrap.find_all('a', class_='tgme_widget_message_link')
                for link in other_links:
                    href = link.get('href', '')
                    if href and not href.startswith('https://t.me/') and href not in post['links']:
                        post['links'].append(href)

                # Добавляем пост если есть данные
                if (post['text'] or post['photo_urls'] or 
                    post['video_urls'] or post['links']):
                    all_posts.append(post)
                    print(f"  Добавлен пост {post['id']}: {len(post['photo_urls'])} фото, {len(post['video_urls'])} видео")

            # Ищем кнопку "Загрузить предыдущие"
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
    """Обновляет кеш, сохраняя только актуальные данные."""
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)

    # Загружаем существующий кеш
    cached_posts = []
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cached_posts = json.load(f)
                print(f"Загружен существующий кеш: {len(cached_posts)} постов")
                
                # ОБРАБОТКА СТАРОГО ФОРМАТА: преобразуем старые одиночные URL в списки
                for post in cached_posts:
                    if 'photo_url' in post and isinstance(post['photo_url'], str):
                        post['photo_urls'] = [post['photo_url']] if post['photo_url'] else []
                        del post['photo_url']
                    elif 'photo_urls' not in post:
                        post['photo_urls'] = []
                    
                    if 'video_url' in post and isinstance(post['video_url'], str):
                        post['video_urls'] = [post['video_url']] if post['video_url'] else []
                        del post['video_url']
                    elif 'video_urls' not in post:
                        post['video_urls'] = []
                        
        except Exception as e:
            print(f"Ошибка при загрузке кеша: {e}")

    # Создаем словарь для быстрого поиска
    existing_ids = {p.get('id'): i for i, p in enumerate(cached_posts) if p.get('id')}

    # Объединяем старые и новые посты
    for post in new_posts:
        post_id = post.get('id')
        if post_id and post_id in existing_ids:
            cached_posts[existing_ids[post_id]] = post
        else:
            cached_posts.insert(0, post)

    # Ограничиваем количество и сохраняем только уникальные
    unique_posts = []
    seen_ids = set()
    for post in cached_posts:
        post_id = post.get('id')
        if post_id and post_id not in seen_ids:
            seen_ids.add(post_id)
            unique_posts.append(post)
    
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
    print("Telegram Channel Parser v2.0")
    print(f"Канал: {CHANNEL_URL}")
    print(f"Время запуска: {datetime.now().isoformat()}")
    print("=" * 50)
    
    posts = parse_telegram_channel()
    
    if posts:
        cached = update_cache(posts)
        
        print("\n" + "=" * 50)
        print("СТАТИСТИКА:")
        print(f"Получено новых постов: {len(posts)}")
        print(f"Всего в кеше: {len(cached)}")
        
        # Обновленная статистика
        total_photos = sum(len(p.get('photo_urls', [])) for p in posts)
        total_videos = sum(len(p.get('video_urls', [])) for p in posts)
        posts_with_photos = sum(1 for p in posts if p.get('photo_urls'))
        posts_with_videos = sum(1 for p in posts if p.get('video_urls'))
        posts_with_links = sum(1 for p in posts if p.get('links'))
        
        print(f"Всего фото найдено: {total_photos}")
        print(f"Всего видео найдено: {total_videos}")
        print(f"Постов с фото: {posts_with_photos}")
        print(f"Постов с видео: {posts_with_videos}")
        print(f"Постов со ссылками: {posts_with_links}")
        
        # Дополнительная статистика
        posts_with_multiple_photos = sum(1 for p in posts if len(p.get('photo_urls', [])) > 1)
        posts_with_multiple_videos = sum(1 for p in posts if len(p.get('video_urls', [])) > 1)
        print(f"Постов с несколькими фото: {posts_with_multiple_photos}")
        print(f"Постов с несколькими видео: {posts_with_multiple_videos}")
        print("=" * 50)
        
        # Сохраняем отдельный файл с последними 10 постами
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
