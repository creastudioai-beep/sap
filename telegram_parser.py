#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 SochiAutoParts Telegram Parser v4.2
Исправлено: извлечение даты публикации из <time datetime="...">
Production-ready: retry-логика, атомарная запись, media_map, гарантированный сбор N постов.
"""

import json
import os
import re
import sys
import time
import random
import logging
import logging.handlers
import tempfile
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# =============================================================================
# 📋 КОНФИГУРАЦИЯ
# =============================================================================

CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/s/sochiautoparts")
PARSE_LIMIT = int(os.getenv("PARSE_LIMIT", "1000"))
CACHE_LIMIT = int(os.getenv("CACHE_LIMIT", "1000"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
REQUEST_DELAY_MIN = float(os.getenv("REQUEST_DELAY_MIN", "0.8"))
REQUEST_DELAY_MAX = float(os.getenv("REQUEST_DELAY_MAX", "1.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

CACHE_FILE = DATA_DIR / "cached_posts.json"
MEDIA_MAP_FILE = DATA_DIR / "media_map.json"
LATEST_FILE = DATA_DIR / "latest_posts.json"
LOG_FILE = DATA_DIR / "parser.log"

# =============================================================================
# 📝 ЛОГИРОВАНИЕ
# =============================================================================

def setup_logger() -> logging.Logger:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger("telegram_parser")
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    file_handler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()

# =============================================================================
# 🔧 УТИЛИТЫ
# =============================================================================

def atomic_write(filepath: Path, data) -> bool:
    """Атомарная запись файла через временный файл."""
    try:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=filepath.parent, suffix='.tmp')
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
            shutil.move(tmp_path, filepath)
            logger.debug(f"💾 Сохранён: {filepath.name}")
            return True
        except:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise
    except Exception as e:
        logger.error(f"❌ Ошибка записи {filepath.name}: {e}")
        return False

def get_user_agent() -> str:
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    ]
    return random.choice(agents)

def jitter_delay():
    delay = random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX)
    time.sleep(delay)

def extract_bg_image(style: str) -> Optional[str]:
    """Надёжное извлечение URL из background-image."""
    if not style:
        return None
    match = re.search(r'background-image:\s*url\(["\']?(.*?)["\']?\)', style, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

def is_valid_url(url: str) -> bool:
    try:
        parsed = urlparse(url)
        return parsed.scheme in ('http', 'https') and bool(parsed.netloc)
    except:
        return False

def extract_post_id(wrap) -> Optional[str]:
    """Извлечение ID поста."""
    msg_div = wrap.find('div', class_='tgme_widget_message')
    if msg_div:
        post_id = msg_div.get('data-post')
        if post_id:
            return post_id.strip()
    elem_with_id = wrap.find(attrs={'data-post': True})
    if elem_with_id:
        return elem_with_id['data-post'].strip()
    return None

def fnv1a_hash_32(s: str) -> int:
    """FNV-1a 32-bit hash."""
    hash_val = 2166136261
    for char in s:
        hash_val ^= ord(char)
        hash_val = (hash_val * 16777619) & 0xFFFFFFFF
    return hash_val

def to_base36(n: int) -> str:
    """Convert to base36."""
    if n == 0:
        return '0'
    digits = []
    while n:
        n, r = divmod(n, 36)
        digits.append('0123456789abcdefghijklmnopqrstuvwxyz'[r])
    return ''.join(reversed(digits))

def generate_media_hash(url: str) -> str:
    """Генерация хеша медиа (совместимо с Worker)."""
    if not url or not isinstance(url, str):
        return '0'
    h = fnv1a_hash_32(url)
    return to_base36(h)

# =============================================================================
# 🌐 СЕТЬ С RETRY
# =============================================================================

@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
    reraise=True
)
def fetch_page(session: requests.Session, url: str) -> str:
    headers = {
        'User-Agent': get_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    }
    response = session.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    
    if len(response.text) < 500:
        raise ValueError("Response too short, possible ban or captcha")
    
    return response.text

# =============================================================================
# 🔍 ПАРСИНГ
# =============================================================================

def parse_post(wrap) -> Optional[Dict]:
    """Парсинг одного поста. Корректно извлекает дату публикации."""
    try:
        post_id = extract_post_id(wrap)
        if not post_id:
            return None
        
        post = {
            'id': post_id,
            'date': '',           # ISO дата публикации
            'text': '',
            'photo_urls': [],
            'video_urls': [],
            'links': [],
            'views': None,
        }
        
        # ---------- ДАТА ПУБЛИКАЦИИ (ИСПРАВЛЕНО) ----------
        # Ищем любой тег <time> с атрибутом datetime
        time_tag = wrap.find('time', attrs={'datetime': True})
        if time_tag:
            post['date'] = time_tag['datetime']
        else:
            # fallback: ищем ссылку с классом tgme_widget_message_date и внутри time
            date_link = wrap.find('a', class_='tgme_widget_message_date')
            if date_link:
                inner_time = date_link.find('time')
                if inner_time and inner_time.get('datetime'):
                    post['date'] = inner_time['datetime']
            # если всё равно нет — остаётся пустая строка
        
        # ---------- ПРОСМОТРЫ ----------
        views_elem = wrap.find('span', class_='tgme_widget_message_views')
        if views_elem:
            views_text = views_elem.get_text(strip=True)
            try:
                if 'K' in views_text:
                    post['views'] = int(float(views_text.replace('K', '')) * 1000)
                else:
                    post['views'] = int(views_text)
            except:
                pass
        
        # ---------- ТЕКСТ И ССЫЛКИ ----------
        text_elem = wrap.find('div', class_='tgme_widget_message_text')
        if text_elem:
            for br in text_elem.find_all('br'):
                br.replace_with('\n')
            post['text'] = text_elem.get_text(separator='\n', strip=True)
            
            for link in text_elem.find_all('a', href=True):
                href = link['href']
                # Исключаем внутренние ссылки Telegram
                if href and not href.startswith('https://t.me/') and is_valid_url(href):
                    if href not in post['links']:
                        post['links'].append(href)
        
        # ---------- ФОТО ----------
        for pw in wrap.find_all('a', class_='tgme_widget_message_photo_wrap'):
            photo_url = extract_bg_image(pw.get('style', ''))
            if photo_url and is_valid_url(photo_url) and photo_url not in post['photo_urls']:
                post['photo_urls'].append(photo_url)
        
        # ---------- ВИДЕО ----------
        for vw in wrap.find_all('div', class_='tgme_widget_message_video_wrap'):
            video_tag = vw.find('video')
            if video_tag:
                for src in [video_tag.get('src'), 
                           video_tag.find('source').get('src') if video_tag.find('source') else None]:
                    if src and is_valid_url(src) and src not in post['video_urls']:
                        post['video_urls'].append(src)
        
        # Round video
        for rv in wrap.find_all('video', class_='tgme_widget_message_roundvideo'):
            if rv.get('src'):
                src = rv['src']
                if is_valid_url(src) and src not in post['video_urls']:
                    post['video_urls'].append(src)
        
        # ---------- ПРЕВЬЮ ССЫЛОК ----------
        for lp in wrap.find_all('a', class_='tgme_widget_message_link_preview'):
            href = lp.get('href', '')
            if href and not href.startswith('https://t.me/') and is_valid_url(href):
                if href not in post['links']:
                    post['links'].append(href)
        
        has_content = post['text'] or post['photo_urls'] or post['video_urls'] or post['links']
        return post if has_content else None
        
    except Exception as e:
        logger.warning(f"⚠️ Ошибка парсинга поста: {e}")
        return None

def load_cache() -> Dict[str, Dict]:
    """Загрузка кеша."""
    cache = {}
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    if isinstance(item, dict) and item.get('id'):
                        cache[item['id']] = item
            logger.info(f"📦 Загружен кеш: {len(cache)} постов")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки кеша: {e}")
    return cache

def generate_media_map(posts: List[Dict]) -> Dict[str, str]:
    """Генерация media_map."""
    media_map = {}
    for post in posts:
        for url in post.get('photo_urls', []) + post.get('video_urls', []):
            if url:
                h = generate_media_hash(url)
                media_map[h] = url
    return media_map

def save_results(cache: Dict[str, Dict]) -> bool:
    """Сохранение всех результатов."""
    posts = list(cache.values())
    sorted_posts = sorted(posts, key=lambda p: p.get('id', ''), reverse=True)
    final_posts = sorted_posts[:CACHE_LIMIT]
    
    success = True
    
    if not atomic_write(CACHE_FILE, final_posts):
        success = False
    
    media_map = generate_media_map(final_posts)
    if not atomic_write(MEDIA_MAP_FILE, media_map):
        success = False
    
    latest = sorted_posts[:10]
    if not atomic_write(LATEST_FILE, latest):
        success = False
    
    if success:
        logger.info(f"✅ Результаты сохранены: {len(final_posts)} постов, {len(media_map)} медиа")
    
    return success

def parse_channel() -> List[Dict]:
    """
    Парсинг канала с гарантированным получением PARSE_LIMIT постов.
    Всегда парсит с начала и собирает указанное количество.
    """
    logger.info(f"🚀 Парсинг: {CHANNEL_URL}")
    logger.info(f"📊 Лимит: {PARSE_LIMIT} постов")
    
    cache = load_cache()
    all_posts: List[Dict] = []
    collected_ids: Set[str] = set()
    next_url = CHANNEL_URL
    
    session = requests.Session()
    pages_loaded = 0
    
    try:
        while len(all_posts) < PARSE_LIMIT and next_url:
            logger.info(f"📄 Страница {pages_loaded + 1}: {next_url}")
            
            try:
                html = fetch_page(session, next_url)
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки страницы: {e}")
                if pages_loaded > 0:
                    logger.warning("⚠️ Продолжаем с уже собранными постами")
                    break
                raise
            
            soup = BeautifulSoup(html, 'html.parser')
            pages_loaded += 1
            
            wraps = soup.find_all('div', class_='tgme_widget_message_wrap')
            
            if not wraps:
                logger.warning("⚠️ Посты не найдены на странице")
                break
            
            logger.info(f"🔍 Найдено элементов: {len(wraps)}")
            
            page_new = 0
            for idx, wrap in enumerate(wraps):
                if len(all_posts) >= PARSE_LIMIT:
                    logger.info(f"⏹ Достигнут лимит: {PARSE_LIMIT} постов")
                    next_url = None
                    break
                
                post = parse_post(wrap)
                if not post:
                    continue
                
                post_id = post['id']
                
                if post_id not in collected_ids:
                    all_posts.append(post)
                    collected_ids.add(post_id)
                    cache[post_id] = post
                    page_new += 1
                    # Отображаем дату в логе, если она есть
                    date_info = post.get('date', 'без даты')
                    logger.info(f"✓ Пост {post_id} | {date_info}")
                else:
                    logger.debug(f"⊘ Дубликат: {post_id}")
            
            logger.info(f"📈 Страница {pages_loaded}: новых={page_new}, всего={len(all_posts)}")
            
            if next_url:
                load_more = soup.find('a', class_='tme_messages_more')
                if load_more and load_more.get('href'):
                    next_url = urljoin('https://t.me', load_more['href'])
                    jitter_delay()
                else:
                    next_url = None
                    logger.info("ℹ️ Кнопка 'Load more' не найдена")
        
        logger.info(f"✅ Парсинг завершён. Собрано: {len(all_posts)} постов")
        
        save_results(cache)
        
        return all_posts
        
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        save_results(cache)
        raise

def print_statistics(posts: List[Dict]):
    """Вывод статистики."""
    if not posts:
        print("\n📊 Нет постов")
        return
    
    with_photos = sum(1 for p in posts if p.get('photo_urls'))
    with_videos = sum(1 for p in posts if p.get('video_urls'))
    with_links = sum(1 for p in posts if p.get('links'))
    
    total_photos = sum(len(p.get('photo_urls', [])) for p in posts)
    total_videos = sum(len(p.get('video_urls', [])) for p in posts)
    
    # Количество постов с датой
    with_date = sum(1 for p in posts if p.get('date'))
    
    print("\n" + "=" * 60)
    print("📊 СТАТИСТИКА")
    print("=" * 60)
    print(f"📝 Постов: {len(posts)}")
    print(f"📅 С датой: {with_date}")
    print(f"📷 С фото: {with_photos} (всего {total_photos})")
    print(f"🎥 С видео: {with_videos} (всего {total_videos})")
    print(f"🔗 Со ссылками: {with_links}")
    print("=" * 60)
    
    print("\n🆕 Последние посты:")
    for post in sorted(posts, key=lambda p: p.get('id', ''), reverse=True)[:5]:
        text_preview = post.get('text', '')[:60].replace('\n', ' ')
        print(f"  • {post['id']} | {post.get('date', 'без даты')} | {text_preview}...")

# =============================================================================
# 🎯 MAIN
# =============================================================================

def main():
    print("\n" + "=" * 60)
    print("🤖 SOCHIAUTOPARTS Telegram Parser v4.2")
    print("=" * 60)
    print(f"🔗 Канал: {CHANNEL_URL}")
    print(f"📊 Лимит: {PARSE_LIMIT}")
    print(f"📁 Data: {DATA_DIR}")
    print(f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60 + "\n")
    
    try:
        posts = parse_channel()
        print_statistics(posts)
        
        print("\n✅ Готово!")
        print(f"📄 Лог: {LOG_FILE}")
        print(f"💾 Кеш: {CACHE_FILE}")
        print(f"🗺️ MediaMap: {MEDIA_MAP_FILE}")
        print(f"📝 Latest: {LATEST_FILE}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Прервано пользователем")
        return 1
    except Exception as e:
        logger.exception(f"❌ Фатальная ошибка: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
