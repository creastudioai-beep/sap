import json
import os
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
import requests

# =============================================================================
# КОНФИГУРАЦИЯ
# =============================================================================
CHANNEL_URL = "https://t.me/s/sochiautoparts"

PARSE_LIMIT = 3500   # Сколько постов парсить за один запуск (для скорости)
CACHE_LIMIT = 3500   # Сколько постов хранить в кеше (история)

CACHE_FILE = "data/cached_posts.json"
LATEST_FILE = "data/latest_posts.json"

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def extract_bg_image(style):
    """Извлекает URL изображения из CSS background-image."""
    if not style:
        return ''
    # Telegram использует format: background-image:url('https://...')
    match = re.search(r"background-image:\s*url\(['\"]?(.*?)['\"]?\)", style)
    if match:
        return match.group(1)
    # Fallback для простого url('...')
    match = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
    if match:
        return match.group(1)
    return ''


def parse_telegram_channel():
    """Парсит последние посты из публичного Telegram канала."""
    all_posts = []
    next_url = CHANNEL_URL
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }

    print(f"🚀 Начинаем парсинг канала: {CHANNEL_URL}")
    print(f"📊 Лимит парсинга: {PARSE_LIMIT} постов")
    print("-" * 50)

    try:
        session = requests.Session()
        pages_loaded = 0

        while len(all_posts) < PARSE_LIMIT and next_url:
            print(f"📄 Загружаем страницу {pages_loaded + 1}: {next_url}")
            response = session.get(next_url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')
            pages_loaded += 1

            # Ищем все сообщения
            message_wrappers = soup.find_all('div', class_='tgme_widget_message_wrap')
            print(f"   Найдено сообщений на странице: {len(message_wrappers)}")

            for wrap in message_wrappers:
                # Проверяем лимит парсинга
                if len(all_posts) >= PARSE_LIMIT:
                    print(f"   ⏹ Достигнут лимит парсинга: {PARSE_LIMIT} постов")
                    break

                post = {
                    'id': None,
                    'date': '',
                    'text': '',
                    'photo_urls': [],
                    'video_urls': [],
                    'links': [],
                    'parsed_at': datetime.now().isoformat()
                }

                # Извлекаем ID сообщения
                message_div = wrap.find('div', class_='tgme_widget_message')
                if message_div and 'data-post' in message_div.attrs:
                    post['id'] = message_div['data-post']

                # Извлекаем дату
                date_elem = wrap.find('time', class_='datetime')
                if date_elem and date_elem.get('datetime'):
                    post['date'] = date_elem['datetime']
                else:
                    date_elem = wrap.find('a', class_='tgme_widget_message_date')
                    if date_elem:
                        time_elem = date_elem.find('time')
                        if time_elem and time_elem.get('datetime'):
                            post['date'] = time_elem['datetime']

                # Извлекаем текст
                text_elem = wrap.find('div', class_='tgme_widget_message_text')
                if text_elem:
                    for br in text_elem.find_all('br'):
                        br.replace_with('\n')
                    post['text'] = text_elem.get_text().strip()

                    # Извлекаем все ссылки из текста
                    for link in text_elem.find_all('a', href=True):
                        href = link.get('href', '')
                        if href and not href.startswith('https://t.me/') and href not in post['links']:
                            post['links'].append(href)

                # === ИЗВЛЕЧЕНИЕ ФОТО ===
                photo_wraps = wrap.find_all('a', class_='tgme_widget_message_photo_wrap')
                for photo_wrap in photo_wraps:
                    style = photo_wrap.get('style', '')
                    photo_url = extract_bg_image(style)
                    if photo_url and photo_url not in post['photo_urls']:
                        post['photo_urls'].append(photo_url)
                
                # Service photo
                service_photo = wrap.find('img', class_='tgme_widget_message_service_photo')
                if service_photo and service_photo.get('src'):
                    if service_photo['src'] not in post['photo_urls']:
                        post['photo_urls'].append(service_photo['src'])

                # === ИЗВЛЕЧЕНИЕ ВИДЕО ===
                video_wraps = wrap.find_all('div', class_='tgme_widget_message_video_wrap')
                for video_wrap in video_wraps:
                    video_tag = video_wrap.find('video', class_='tgme_widget_message_video')
                    if video_tag:
                        if video_tag.get('src'):
                            if video_tag['src'] not in post['video_urls']:
                                post['video_urls'].append(video_tag['src'])
                        source_tag = video_tag.find('source')
                        if source_tag and source_tag.get('src'):
                            if source_tag['src'] not in post['video_urls']:
                                post['video_urls'].append(source_tag['src'])
                
                # Круглые видео
                round_videos = wrap.find_all('video', class_='tgme_widget_message_roundvideo')
                for rv in round_videos:
                    if rv.get('src') and rv['src'] not in post['video_urls']:
                        post['video_urls'].append(rv['src'])

                # Дополнительные ссылки
                other_links = wrap.find_all('a', class_='tgme_widget_message_link_preview')
                for link in other_links:
                    href = link.get('href', '')
                    if href and not href.startswith('https://t.me/') and href not in post['links']:
                        post['links'].append(href)

                # Добавляем пост если есть данные
                if post['text'] or post['photo_urls'] or post['video_urls'] or post['links']:
                    all_posts.append(post)
                    media_info = []
                    if post['photo_urls']:
                        media_info.append(f"📷{len(post['photo_urls'])}")
                    if post['video_urls']:
                        media_info.append(f"🎥{len(post['video_urls'])}")
                    print(f"   ✓ Пост {post['id']} | {post['date']} | {' '.join(media_info)}")

            # Ищем кнопку "Загрузить предыдущие"
            load_more = soup.find('a', class_='tme_messages_more')
            if load_more and load_more.get('href'):
                next_url = f"https://t.me{load_more['href']}"
            else:
                next_url = None
                print("   ℹ Больше сообщений не найдено")

            if next_url:
                time.sleep(0.5)  # Небольшая задержка между запросами

    except requests.RequestException as e:
        print(f"❌ Ошибка при загрузке страницы: {e}")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")

    print("-" * 50)
    print(f"✅ Собрано постов: {len(all_posts)}")
    return all_posts


def update_cache(new_posts):
    """Обновляет кеш, сохраняя только актуальные данные с учётом CACHE_LIMIT."""
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)

    cached_posts = []
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cached_posts = json.load(f)
                print(f"📦 Загружен существующий кеш: {len(cached_posts)} постов")
        except Exception as e:
            print(f"⚠ Ошибка при загрузке кеша: {e}")

    # Создаём словарь существующих постов для быстрого обновления
    existing_ids = {p.get('id'): i for i, p in enumerate(cached_posts) if p.get('id')}

    # Обновляем или добавляем новые посты
    for post in new_posts:
        post_id = post.get('id')
        if post_id and post_id in existing_ids:
            # Обновляем существующий пост
            cached_posts[existing_ids[post_id]] = post
        else:
            # Добавляем новый пост в начало
            cached_posts.insert(0, post)

    # Удаляем дубликаты (на случай если insert создал дубли)
    unique_posts = []
    seen_ids = set()
    for post in cached_posts:
        post_id = post.get('id')
        if post_id and post_id not in seen_ids:
            seen_ids.add(post_id)
            unique_posts.append(post)
        elif not post_id:
            # Посты без ID тоже сохраняем
            unique_posts.append(post)

    # Применяем CACHE_LIMIT
    final_posts = unique_posts[:CACHE_LIMIT]
    
    removed_count = len(unique_posts) - len(final_posts)
    if removed_count > 0:
        print(f"🗑 Удалено старых постов (превышен CACHE_LIMIT): {removed_count}")

    # Сохраняем кеш
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_posts, f, ensure_ascii=False, indent=2, default=str)
        print(f"💾 Кеш сохранён. Всего постов: {len(final_posts)} / {CACHE_LIMIT}")
    except Exception as e:
        print(f"❌ Ошибка при сохранении кеша: {e}")

    return final_posts


def save_latest_posts(posts, count=10):
    """Сохраняет последние N постов в отдельный файл."""
    try:
        with open(LATEST_FILE, 'w', encoding='utf-8') as f:
            json.dump(posts[:count], f, ensure_ascii=False, indent=2, default=str)
        print(f"📄 Создан файл с последними {count} постами: {LATEST_FILE}")
    except Exception as e:
        print(f"⚠ Не удалось создать файл с последними постами: {e}")


def print_statistics(posts):
    """Выводит статистику по собранным постам."""
    if not posts:
        return

    posts_with_photos = sum(1 for p in posts if p.get('photo_urls'))
    posts_with_videos = sum(1 for p in posts if p.get('video_urls'))
    posts_with_links = sum(1 for p in posts if p.get('links'))
    total_photos = sum(len(p.get('photo_urls', [])) for p in posts)
    total_videos = sum(len(p.get('video_urls', [])) for p in posts)
    total_links = sum(len(p.get('links', [])) for p in posts)

    print("\n" + "=" * 50)
    print("📊 СТАТИСТИКА ПАРСИНГА")
    print("=" * 50)
    print(f"Постов обработано: {len(posts)}")
    print(f"Постов с фото: {posts_with_photos} (всего {total_photos} фото)")
    print(f"Постов с видео: {posts_with_videos} (всего {total_videos} видео)")
    print(f"Постов со ссылками: {posts_with_links} (всего {total_links} ссылок)")
    print("=" * 50)


def main():
    """Основная функция парсера."""
    print("\n" + "=" * 50)
    print("🤖 Telegram Channel Parser")
    print("=" * 50)
    print(f"Канал: {CHANNEL_URL}")
    print(f"PARSE_LIMIT: {PARSE_LIMIT} постов за запуск")
    print(f"CACHE_LIMIT: {CACHE_LIMIT} постов в истории")
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50 + "\n")

    # Парсим канал
    posts = parse_telegram_channel()

    if posts:
        # Обновляем кеш
        cached = update_cache(posts)
        
        # Выводим статистику
        print_statistics(posts)
        
        # Сохраняем последние посты
        save_latest_posts(cached, count=10)
        
        print(f"\n⏱ Время завершения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("✅ Парсинг завершён успешно!")
    else:
        print("\n❌ Не удалось получить посты из канала.")
        print("Проверьте подключение к интернету и доступность канала.")


if __name__ == "__main__":
    main()
