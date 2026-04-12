#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 SochiAutoParts Telegram Parser v2.1
Исправлены: logging.handlers, Pydantic ConfigDict, совместимость с V2.
"""

import json
import os
import re
import sys
import time
import random
import logging
import logging.handlers  # 🔧 Явный импорт handlers
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict  # 🔧 SettingsConfigDict
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# =============================================================================
# 📋 КОНФИГУРАЦИЯ (Pydantic V2 Compatible)
# =============================================================================

class Settings(BaseSettings):
    channel_url: str = "https://t.me/s/sochiautoparts"
    parse_limit: int = 3000
    cache_limit: int = 3000
    request_timeout: int = 30
    request_delay_min: float = 0.8
    request_delay_max: float = 1.5
    max_retries: int = 5
    data_dir: Path = Path("data")
    
    # ✅ Pydantic V2: используем model_config вместо class Config
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

config = Settings()

# Пути к файлам
CACHE_FILE = config.data_dir / "cached_posts.json"
MEDIA_MAP_FILE = config.data_dir / "media_map.json"
LATEST_FILE = config.data_dir / "latest_posts.json"
LOG_FILE = config.data_dir / "parser.log"

# =============================================================================
# 📝 ЛОГИРОВАНИЕ
# =============================================================================

def setup_logger() -> logging.Logger:
    config.data_dir.mkdir(parents=True, exist_ok=True)
    
    logger = logging.getLogger("telegram_parser")
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # ✅ Теперь logging.handlers доступен
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
# 📦 МОДЕЛИ ДАННЫХ
# =============================================================================

class Post(BaseModel):
    id: str
    date: str = ""
    text: str = ""
    photo_urls: List[str] = Field(default_factory=list)
    video_urls: List[str] = Field(default_factory=list)
    links: List[str] = Field(default_factory=list)
    views: Optional[int] = None
    parsed_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    
    @field_validator('id')
    @classmethod
    def validate_id(cls, v: str) -> str:
        if not v:
            raise ValueError("Post ID cannot be empty")
        return v
    
    def has_content(self) -> bool:
        return bool(self.text or self.photo_urls or self.video_urls or self.links)
    
    def to_dict(self) -> dict:
        return self.model_dump()

# =============================================================================
# 🔧 ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def get_user_agent() -> str:
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    ]
    return random.choice(agents)

def jitter_delay():
    delay = random.uniform(config.request_delay_min, config.request_delay_max)
    logger.debug(f"⏳ Задержка: {delay:.2f}s")
    time.sleep(delay)

def extract_bg_image(style: str) -> Optional[str]:
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

def generate_media_hash(url: str) -> str:
    if not url:
        return '0'
    hash_val = 2166136261
    for char in url:
        hash_val ^= ord(char)
        hash_val = (hash_val * 16777619) & 0xFFFFFFFF
    if hash_val == 0:
        return '0'
    digits = []
    while hash_val:
        hash_val, r = divmod(hash_val, 36)
        digits.append('0123456789abcdefghijklmnopqrstuvwxyz'[r])
    return ''.join(reversed(digits))

# =============================================================================
# 🌐 СЕТЕВОЙ СЛОЙ
# =============================================================================

@retry(
    stop=stop_after_attempt(config.max_retries),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
    reraise=True
)
def fetch_page(session: requests.Session, url: str) -> str:
    headers = {
        'User-Agent': get_user_agent(),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    response = session.get(url, headers=headers, timeout=config.request_timeout)
    response.raise_for_status()
    return response.text

# =============================================================================
# 🔍 ПАРСИНГ
# =============================================================================

def parse_post(wrap) -> Optional[Post]:
    try:
        message_div = wrap.find('div', class_='tgme_widget_message')
        if not message_div:
            return None
        
        post_id = message_div.get('data-post')
        if not post_id:
            return None
        
        post = Post(id=post_id)
        
        # 📅 Дата
        date_elem = wrap.find('time', class_='datetime')
        if date_elem and date_elem.get('datetime'):
            post.date = date_elem['datetime']
        
        # 👁️ Просмотры
        views_elem = wrap.find('span', class_='tgme_widget_message_views')
        if views_elem:
            views_text = views_elem.get_text(strip=True)
            try:
                if 'K' in views_text:
                    post.views = int(float(views_text.replace('K', '')) * 1000)
                else:
                    post.views = int(views_text)
            except:
                pass
        
        # 📝 Текст и ссылки
        text_elem = wrap.find('div', class_='tgme_widget_message_text')
        if text_elem:
            for br in text_elem.find_all('br'):
                br.replace_with('\n')
            post.text = text_elem.get_text(separator='\n', strip=True)
            
            for link in text_elem.find_all('a', href=True):
                href = link['href']
                if href and not href.startswith('https://t.me/') and is_valid_url(href):
                    if href not in post.links:
                        post.links.append(href)
        
        # 📷 Фото
        photo_wraps = wrap.find_all('a', class_='tgme_widget_message_photo_wrap')
        for pw in photo_wraps:
            style = pw.get('style', '')
            photo_url = extract_bg_image(style)
            if photo_url and is_valid_url(photo_url) and photo_url not in post.photo_urls:
                post.photo_urls.append(photo_url)
        
        # 🎥 Видео
        video_wraps = wrap.find_all('div', class_='tgme_widget_message_video_wrap')
        for vw in video_wraps:
            video_tag = vw.find('video')
            if video_tag:
                if video_tag.get('src'):
                    src = video_tag['src']
                    if is_valid_url(src) and src not in post.video_urls:
                        post.video_urls.append(src)
                source_tag = video_tag.find('source')
                if source_tag and source_tag.get('src'):
                    src = source_tag['src']
                    if is_valid_url(src) and src not in post.video_urls:
                        post.video_urls.append(src)
        
        # 🎬 Round video
        round_videos = wrap.find_all('video', class_='tgme_widget_message_roundvideo')
        for rv in round_videos:
            if rv.get('src'):
                src = rv['src']
                if is_valid_url(src) and src not in post.video_urls:
                    post.video_urls.append(src)
        
        # 🔗 Preview links
        link_previews = wrap.find_all('a', class_='tgme_widget_message_link_preview')
        for lp in link_previews:
            href = lp.get('href', '')
            if href and not href.startswith('https://t.me/') and is_valid_url(href):
                if href not in post.links:
                    post.links.append(href)
        
        return post if post.has_content() else None
        
    except Exception as e:
        logger.warning(f"⚠️ Ошибка парсинга поста: {e}")
        return None

def load_cache() -> Dict[str, Post]:
    cache = {}
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for item in data:
                    try:
                        post = Post(**item)
                        cache[post.id] = post
                    except:
                        continue
            logger.info(f"📦 Загружен кеш: {len(cache)} постов")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки кеша: {e}")
    return cache

def save_cache(posts: List[Post]):
    try:
        sorted_posts = sorted(posts, key=lambda p: p.id, reverse=True)
        final_posts = sorted_posts[:config.cache_limit]
        
        with open(CACHE_FILE, 'w', encoding='utf-8') as f:
            json.dump([p.to_dict() for p in final_posts], f, ensure_ascii=False, indent=2)
        
        logger.info(f"💾 Кеш сохранён: {len(final_posts)} постов")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения кеша: {e}")

def save_media_map(posts: List[Post]):
    media_map = {}
    for post in posts:
        for url in post.photo_urls + post.video_urls:
            h = generate_media_hash(url)
            media_map[h] = url
    
    try:
        with open(MEDIA_MAP_FILE, 'w', encoding='utf-8') as f:
            json.dump(media_map, f, ensure_ascii=False, indent=2, sort_keys=True)
        logger.info(f"🗺️ MediaMap сохранён: {len(media_map)} медиа")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения media_map: {e}")

def save_latest(posts: List[Post], count: int = 10):
    try:
        latest = sorted(posts, key=lambda p: p.id, reverse=True)[:count]
        with open(LATEST_FILE, 'w', encoding='utf-8') as f:
            json.dump([p.to_dict() for p in latest], f, ensure_ascii=False, indent=2)
        logger.info(f"📝 Latest сохранён: {len(latest)} постов")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения latest: {e}")

def parse_channel(incremental: bool = True) -> List[Post]:
    logger.info(f"🚀 Начинаем парсинг: {config.channel_url}")
    logger.info(f"📊 Лимит: {config.parse_limit}, Режим: {'инкрементальный' if incremental else 'полный'}")
    
    cache = load_cache() if incremental else {}
    known_ids = set(cache.keys())
    
    all_posts: List[Post] = []
    new_posts: List[Post] = []
    next_url = config.channel_url
    
    session = requests.Session()
    pages_loaded = 0
    
    try:
        while len(all_posts) < config.parse_limit and next_url:
            logger.info(f"📄 Страница {pages_loaded + 1}: {next_url}")
            
            html = fetch_page(session, next_url)
            soup = BeautifulSoup(html, 'html.parser')
            pages_loaded += 1
            
            wraps = soup.find_all('div', class_='tgme_widget_message_wrap')
            logger.info(f"🔍 Найдено элементов: {len(wraps)}")
            
            if not wraps:
                logger.warning("⚠️ Посты не найдены на странице")
                break
            
            page_new = 0
            for wrap in wraps:
                post = parse_post(wrap)
                if not post:
                    continue
                
                all_posts.append(post)
                
                if post.id not in known_ids:
                    new_posts.append(post)
                    cache[post.id] = post
                    page_new += 1
                    logger.debug(f"✓ Новый пост: {post.id}")
                elif incremental:
                    logger.info(f"🛑 Достигнут известный пост {post.id}, останавливаемся")
                    next_url = None
                    break
            
            logger.info(f"📈 Страница {pages_loaded}: новых постов {page_new}")
            
            if next_url:
                load_more = soup.find('a', class_='tme_messages_more')
                if load_more and load_more.get('href'):
                    next_url = urljoin('https://t.me', load_more['href'])
                    jitter_delay()
                else:
                    next_url = None
                    logger.info("ℹ️ Кнопка 'Load more' не найдена")
        
        logger.info(f"✅ Парсинг завершён. Всего: {len(all_posts)}, Новых: {len(new_posts)}")
        
        final_posts = list(cache.values())
        save_cache(final_posts)
        save_media_map(final_posts)
        save_latest(final_posts)
        
        return new_posts
        
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        raise

def print_statistics(posts: List[Post]):
    if not posts:
        return
    
    with_photos = sum(1 for p in posts if p.photo_urls)
    with_videos = sum(1 for p in posts if p.video_urls)
    with_links = sum(1 for p in posts if p.links)
    
    total_photos = sum(len(p.photo_urls) for p in posts)
    total_videos = sum(len(p.video_urls) for p in posts)
    
    print("\n" + "=" * 60)
    print("📊 СТАТИСТИКА ПАРСИНГА")
    print("=" * 60)
    print(f"📝 Постов обработано: {len(posts)}")
    print(f"📷 С фото: {with_photos} (всего {total_photos})")
    print(f"🎥 С видео: {with_videos} (всего {total_videos})")
    print(f"🔗 Со ссылками: {with_links}")
    print("=" * 60)

# =============================================================================
# 🎯 MAIN
# =============================================================================

def main():
    print("\n" + "=" * 60)
    print("🤖 SOCHIAUTOPARTS Telegram Parser v2.1")
    print("=" * 60)
    print(f"📅 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🔗 Канал: {config.channel_url}")
    print(f"📁 Data dir: {config.data_dir}")
    print("=" * 60 + "\n")
    
    incremental = '--full' not in sys.argv
    
    try:
        new_posts = parse_channel(incremental=incremental)
        print_statistics(new_posts)
        
        print("\n✅ Успешно завершено!")
        print(f"📄 Лог: {LOG_FILE}")
        print(f"💾 Кеш: {CACHE_FILE}")
        print(f"🗺️ MediaMap: {MEDIA_MAP_FILE}")
        print(f"📝 Latest: {LATEST_FILE}")
        
    except KeyboardInterrupt:
        logger.warning("⚠️ Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"❌ Фатальная ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
