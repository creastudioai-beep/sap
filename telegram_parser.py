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
PARSE_LIMIT = 1000
CACHE_LIMIT = 3000
CACHE_FILE = "data/cached_posts.json"
MEDIA_MAP_FILE = "data/media_map.json"  # Новый файл с картой медиа
LATEST_FILE = "data/latest_posts.json"

# =============================================================================
# ХЕШИРОВАНИЕ (FNV-1a + base36) - ИДЕНТИЧНО WORKER
# =============================================================================
def fnv1a_hash_32(s):
    """FNV-1a 32-bit hash algorithm (matches Worker implementation)."""
    hash_val = 2166136261
    for char in s:
        hash_val ^= ord(char)
        hash_val = (hash_val * 16777619) & 0xFFFFFFFF
    return hash_val

def to_base36(n):
    """Convert integer to base36 string."""
    if n == 0:
        return '0'
    digits = []
    while n:
        n, r = divmod(n, 36)
        digits.append('0123456789abcdefghijklmnopqrstuvwxyz'[r])
    return ''.join(reversed(digits))

def generate_media_hash(url):
    """Generate media hash identical to Worker's generateMediaHash()."""
    if not url or not isinstance(url, str):
        return '0'
    h = fnv1a_hash_32(url)
    return to_base36(h)

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================
def extract_bg_image(style):
    """Извлекает URL изображения из CSS background-image."""
    if not style:
        return ''
    match = re.search(r"background-image:\s*url\(['\"]?(.*?)['\"]?\)", style)
    if match:
        return match.group(1)
    match = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
    if match:
        return match.group(1)
    return ''

def generate_media_map(posts):
    """
    Генерирует карту медиа: { hash: url } для всех медиа в постах.
    Сохраняет в data/media_map.json.
    """
    media_map = {}
    total_media = 0
    
    for post in posts:
        if not post:
            continue
        
        # Фото
        for url in post.get('photo_urls', []):
            if url:
                h = generate_media_hash(url)
                media_map[h] = url
                total_media += 1
        
        # Видео
        for url in post.get('video_urls', []):
            if url:
                h = generate_media_hash(url)
                media_map[h] = url
                total_media += 1
    
    # Сохраняем карту
    try:
        os.makedirs(os.path.dirname(MEDIA_MAP_FILE), exist_ok=True)
        with open(MEDIA_MAP_FILE, 'w', encoding='utf-8') as f:
            json.dump(media_map, f, ensure_ascii=False, indent=2, sort_keys=True)
        print(f"🗺️  MediaMap сохранён: {len(media_map)} уникальных медиа ({total_media} всего)")
        print(f"   Файл: {MEDIA_MAP_FILE}")
        return media_map
    except Exception as e:
        print(f"❌ Ошибка при сохранении media_map: {e}")
        return {}

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
            
            message_wrappers = soup.find_all('div', class_='tgme_widget_message_wrap')
            print(f"   🔍 Найдено сообщений: {len(message_wrappers)}")
            
            for wrap in message_wrappers:
                if len(all_posts) >= PARSE_LIMIT:
                    print(f"   ⏹ Достигнут лимит: {PARSE_LIMIT} постов")
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
                
                message_div = wrap.find('div', class_='tgme_widget_message')
                if message_div and 'data-post' in message_div.attrs:
                    post['id'] = message_div['data-post']
                
                date_elem = wrap.find('time', class_='datetime')
                if date_elem and date_elem.get('datetime'):
                    post['date'] = date_elem['datetime']
                else:
                    date_elem = wrap.find('a', class_='tgme_widget_message_date')
                    if date_elem:
                        time_elem = date_elem.find('time')
                        if time_elem and time_elem.get('datetime'):
                            post['date'] = time_elem['datetime']
                
                text_elem = wrap.find('div', class_='tgme_widget_message_text')
                if text_elem:
                    for br in text_elem.find_all('br'):
                        br.replace_with('\n')
                    post['text'] = text_elem.get_text().strip()
                    for link in text_elem.find_all('a', href=True):
                        href = link.get('href', '')
                        if href and not href.startswith('https://t.me/') and href not in post['links']:
                            post['links'].append(href)
                
                # Фото
                photo_wraps = wrap.find_all('a', class_='tgme_widget_message_photo_wrap')
                for photo_wrap in photo_wraps:
                    style = photo_wrap.get('style', '')
                    photo_url = extract_bg_image(style)
                    if photo_url and photo_url not in post['photo_urls']:
                        post['photo_urls'].append(photo_url)
                
                service_photo = wrap.find('img', class_='tgme_widget_message_service_photo')
                if service_photo and service_photo.get('src'):
                    if service_photo['src'] not in post['photo_urls']:
                        post['photo_urls'].append(service_photo['src'])
                
                # Видео
                video_wraps = wrap.find_all('div', class_='tgme_widget_message_video_wrap')
                for video_wrap in video_wraps:
                    video_tag = video_wrap.find('video', class_='tgme_widget_message_video')
                    if video_tag:
                        if video_tag.get('src') and video_tag['src'] not in post['video_urls']:
                            post['video_urls'].append(video_tag['src'])
                        source_tag = video_tag.find('source')
                        if source_tag and source_tag.get('src') and source_tag['src'] not in post['video_urls']:
                            post['video_urls'].append(source_tag['src'])
                
                round_videos = wrap.find_all('video', class_='tgme_widget_message_roundvideo')
                for rv in round_videos:
                    if rv.get('src') and rv['src'] not in post['video_urls']:
                        post['video_urls'].append(rv['src'])
                
                other_links = wrap.find_all('a', class_='tgme_widget_message_link_preview')
                for link in other_links:
                    href = link.get('href', '')
                    if href and not href.startswith('https://t.me/') and href not in post['links']:
                        post['links'].append(href)
                
                if post['text'] or post['photo_urls'] or post['video_urls'] or post['links']:
                    all_posts.append(post)
                    media_info = []
                    if post['photo_urls']:
                        media_info.append(f"📷{len(post['photo_urls'])}")
                    if post['video_urls']:
                        media_info.append(f"🎥{len(post['video_urls'])}")
                    print(f"   ✓ Пост {post['id']} | {post['date']} | {' '.join(media_info)}")
            
            load_more = soup.find('a', class_='tme_messages_more')
            if load_more and load_more.get('href'):
                next_url = f"https://t.me{load_more['href']}"
            else:
                next_url = None
                print("   ℹ Больше сообщений не найдено")
            
            if next_url:
                time.sleep(0.5)
    
    except requests.RequestException as e:
        print(f"❌ Ошибка при загрузке страницы: {e}")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")
    
    print("-" * 50)
    print(f"✅ Собрано постов: {len(all_posts)}")
    return all_posts

def update_cache(new_posts):
    """Обновляет кеш постов."""
    os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
    cached_posts = []
    
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                cached_posts = json.load(f)
            print(f"📦 Загружен кеш: {len(cached_posts)} постов")
        except Exception as e:
            print(f"⚠ Ошибка загрузки кеша: {e}")
    
    existing_ids = {p.get('id'): i for i, p in enumerate(cached_posts) if p.get('id')}
    
    for post in new_posts:
        post_id = post.get('id')
        if post_id and post_id in existing_ids:
            cached_posts[existing_ids[post_id]] = post
        else:
            cached_posts.insert(0, post)
    
    unique_posts = []
    seen_ids = set()
    for post in cached_posts:
        post_id = post.get('id')
        if post_id and post_id not in seen_ids:
            seen_ids.add(post_id)
            unique_posts.append(post)
        elif not post_id:
            unique_posts.append(post)
    
    final_posts = unique_posts[:CACHE_LIMIT]
    removed = len(unique_posts) - len(final_posts)
    if removed > 0:
        print(f"🗑️  Удалено старых постов: {removed}")
    
    try:
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump(final_posts, f, ensure_ascii=False, indent=2, default=str)
        print(f"💾 Кеш сохранён: {len(final_posts)} / {CACHE_LIMIT} постов")
    except Exception as e:
        print(f"❌ Ошибка сохранения кеша: {e}")
    
    return final_posts

def save_latest_posts(posts, count=10):
    """Сохраняет последние N постов."""
    try:
        with open(LATEST_FILE, 'w', encoding='utf-8') as f:
            json.dump(posts[:count], f, ensure_ascii=False, indent=2, default=str)
        print(f"📝 Создан {LATEST_FILE} ({count} постов)")
    except Exception as e:
        print(f"⚠ Ошибка создания {LATEST_FILE}: {e}")

def print_statistics(posts):
    """Выводит статистику."""
    if not posts:
        return
    with_photos = sum(1 for p in posts if p.get('photo_urls'))
    with_videos = sum(1 for p in posts if p.get('video_urls'))
    with_links = sum(1 for p in posts if p.get('links'))
    total_photos = sum(len(p.get('photo_urls', [])) for p in posts)
    total_videos = sum(len(p.get('video_urls', [])) for p in posts)
    total_links = sum(len(p.get('links', [])) for p in posts)
    
    print("\n" + "=" * 50)
    print("📊 СТАТИСТИКА")
    print("=" * 50)
    print(f"Постов: {len(posts)}")
    print(f"С фото: {with_photos} (всего {total_photos})")
    print(f"С видео: {with_videos} (всего {total_videos})")
    print(f"Со ссылками: {with_links} (всего {total_links})")
    print("=" * 50)

def main():
    """Основная функция."""
    print("\n" + "=" * 50)
    print("🤖 SOCHIAUTOPARTS Telegram Parser")
    print("=" * 50)
    print(f"Канал: {CHANNEL_URL}")
    print(f"PARSE_LIMIT: {PARSE_LIMIT}")
    print(f"CACHE_LIMIT: {CACHE_LIMIT}")
    print(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50 + "\n")
    
    posts = parse_telegram_channel()
    
    if posts:
        cached = update_cache(posts)
        
        # 🗺️ Генерируем media_map
        print("\n🔄 Генерация media_map...")
        generate_media_map(cached)
        
        print_statistics(cached)
        save_latest_posts(cached, count=10)
        
        print(f"\n⏱ Завершено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("✅ Парсинг успешен! Файлы:")
        print(f"   - {CACHE_FILE}")
        print(f"   - {MEDIA_MAP_FILE}")
        print(f"   - {LATEST_FILE}")
    else:
        print("\n❌ Не удалось получить посты")

if __name__ == "__main__":
    main()
