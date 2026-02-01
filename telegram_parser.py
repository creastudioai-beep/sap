import json
import os
import re
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import urllib.parse

CHANNEL_URL = "https://t.me/s/sochiautoparts"
MAX_POSTS = 300
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
                    'photo_urls': [],  # Теперь список для всех фото
                    'video_urls': [],  # Теперь список для всех видео
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

                # Извлекаем ВСЕ фото
                # 1. Ищем все элементы с фоновыми изображениями (карусели)
                photo_wraps = wrap.find_all('a', class_='tgme_widget_message_photo_wrap')
                for photo_wrap in photo_wraps:
                    style = photo_wrap.get('style', '')
                    if style:
                        # Ищем все URL в background-image
                        matches = re.findall(r"url\('(.*?)'\)", style)
                        for url in matches:
                            if url and url not in post['photo_urls']:
                                post['photo_urls'].append(url)
                
                # 2. Ищем все отдельные изображения
                img_tags = wrap.find_all('img', class_='tgme_widget_message_photo')
                for img in img_tags:
                    src = img.get('src')
                    if src and src not in post['photo_urls']:
                        post['photo_urls'].append(src)
                
                # 3. Ищем фото в слайдерах (каруселях)
                slide_items = wrap.find_all('div', class_='tgme_widget_message_slide_item')
                for slide in slide_items:
                    # Проверяем фоновое изображение в слайдере
                    slide_style = slide.get('style', '')
                    if slide_style:
                        slide_matches = re.findall(r"url\('(.*?)'\)", slide_style)
                        for url in slide_matches:
                            if url and url not in post['photo_urls']:
                                post['photo_urls'].append(url)
                    
                    # Ищем теги img внутри слайдов
                    slide_imgs = slide.find_all('img')
                    for slide_img in slide_imgs:
                        src = slide_img.get('src')
                        if src and src not in post['photo_urls']:
                            post['photo_urls'].append(src)

                # Извлекаем ВСЕ видео
                # 1. Ищем все теги video
                video_tags = wrap.find_all('video', class_='tgme_widget_message_video')
                for video in video_tags:
                    # Проверяем source внутри video
                    sources = video.find_all('source')
                    for source in sources:
                        src = source.get('src')
                        if src and src not in post['video_urls']:
                            post['video_urls'].append(src)
                    
                    # Проверяем атрибут src у самого тега video
                    video_src = video.get('src')
                    if video_src and video_src not in post['video_urls']:
                        post['video_urls'].append(video_src)
                
                # 2. Ищем видео в слайдерах
                video_slides = wrap.find_all('div', class_='tgme_widget_message_slide_video')
                for video_slide in video_slides:
                    # Проверяем фоновое видео в слайдере
                    slide_style = video_slide.get('style', '')
                    if slide_style:
                        # Ищем видео URL в стилях
                        video_matches = re.findall(r"url\('(.*?)'\)", slide_style)
                        for url in video_matches:
                            if url and url not in post['video_urls']:
                                post['video_urls'].append(url)

                # Ищем дополнительные ссылки
                other_links = wrap.find_all('a', class_='tgme_widget_message_link')
                for link in other_links:
                    href = link.get('href', '')
                    if href and not href.startswith('https://t.me/') and href not in post['links']:
                        post['links'].append(href)

                # Добавляем пост только если есть хоть какие-то данные
                if post['text'] or post['photo_urls'] or post['video_urls'] or post['links']:
                    all_posts.append(post)
                    # Выводим отладочную информацию
                    photo_count = len(post['photo_urls'])
                    video_count = len(post['video_urls'])
                    if photo_count > 0 or video_count > 0:
                        print(f"  Добавлен пост: {post['date']} (фото: {photo_count}, видео: {video_count})")
                    else:
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
                
                # Конвертируем старый формат в новый
                for i, post in enumerate(cached_posts):
                    # Если есть старые поля photo_url/video_url, конвертируем в списки
                    if 'photo_url' in post and 'photo_urls' not in post:
                        cached_posts[i]['photo_urls'] = [post['photo_url']] if post['photo_url'] else []
                        # Удаляем старое поле
                        if 'photo_url' in cached_posts[i]:
                            del cached_posts[i]['photo_url']
                    
                    if 'video_url' in post and 'video_urls' not in post:
                        cached_posts[i]['video_urls'] = [post['video_url']] if post['video_url'] else []
                        # Удаляем старое поле
                        if 'video_url' in cached_posts[i]:
                            del cached_posts[i]['video_url']
                    
                    # Если нет полей photo_urls/video_urls, создаем пустые списки
                    if 'photo_urls' not in cached_posts[i]:
                        cached_posts[i]['photo_urls'] = []
                    if 'video_urls' not in cached_posts[i]:
                        cached_posts[i]['video_urls'] = []
                        
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
    print("Telegram Channel Parser (обновленная версия)")
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
        
        # Подсчитываем посты с медиа
        posts_with_photos = sum(1 for p in posts if p.get('photo_urls') and len(p['photo_urls']) > 0)
        posts_with_videos = sum(1 for p in posts if p.get('video_urls') and len(p['video_urls']) > 0)
        
        # Подсчитываем общее количество медиафайлов
        total_photos = sum(len(p.get('photo_urls', [])) for p in posts)
        total_videos = sum(len(p.get('video_urls', [])) for p in posts)
        
        # Находим пост с максимальным количеством медиа
        max_photos_in_post = max(len(p.get('photo_urls', [])) for p in posts) if posts else 0
        max_videos_in_post = max(len(p.get('video_urls', [])) for p in posts) if posts else 0
        
        posts_with_links = sum(1 for p in posts if p.get('links'))
        
        print(f"Постов с фото: {posts_with_photos}")
        print(f"Постов с видео: {posts_with_videos}")
        print(f"Постов со ссылками: {posts_with_links}")
        print(f"Всего фото: {total_photos}")
        print(f"Всего видео: {total_videos}")
        print(f"Максимум фото в одном посте: {max_photos_in_post}")
        print(f"Максимум видео в одном посте: {max_videos_in_post}")
        
        # Пример вывода информации о постах с несколькими медиа
        print("\nПримеры постов с несколькими медиа:")
        multi_media_posts = [p for p in posts if len(p.get('photo_urls', [])) > 1 or len(p.get('video_urls', [])) > 1]
        for i, post in enumerate(multi_media_posts[:3]):  # Показываем только первые 3
            photo_count = len(post.get('photo_urls', []))
            video_count = len(post.get('video_urls', []))
            print(f"  {i+1}. Пост ID: {post.get('id')} - фото: {photo_count}, видео: {video_count}")
        
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
