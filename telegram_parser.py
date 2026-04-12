#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SOCHIAUTOPARTS Telegram Parser v2.2
Парсер t.me/s/sochiautoparts с поддержкой репостов и всех форматов контента
Совместим с Cloudflare Worker v84.1-AUDITED
"""

import re
import json
import time
import random
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Настройки логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('parser.log', encoding='utf-8', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
CONFIG = {
    'CHANNEL_URL': 'https://t.me/s/sochiautoparts',
    'OUTPUT_FILE': 'cached_posts.json',
    'MEDIA_MAP_FILE': 'media_map.json',
    'MAX_PAGES': 10,
    'POSTS_PER_PAGE': 20,
    'MAX_POSTS': 2000,
    'REQUEST_DELAY': (0.5, 1.5),
    'MAX_RETRIES': 3,
    'RETRY_BACKOFF': 0.3,
    'TIMEOUT': 30,
    'USER_AGENTS': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    ]
}

# Сессия с retry-логикой
def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=CONFIG['MAX_RETRIES'],
        backoff_factor=CONFIG['RETRY_BACKOFF'],
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=['GET']
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

SESSION = create_session()

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

def extract_bg_image(style: Optional[str]) -> str:
    """Извлекает URL изображения из CSS background-image."""
    if not style:
        return ''
    patterns = [
        r'url\(\s*["\']?([^"\')\s]+)["\']?\s*\)',
        r'background-image\s*:\s*url\(\s*["\']?([^"\')\s]+)["\']?\s*\)'
    ]
    for pattern in patterns:
        match = re.search(pattern, style, re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ''

def extract_text_with_formatting(element) -> str:
    """Извлекает текст с сохранением базового форматирования."""
    if not element:
        return ''
    
    # Обработка ссылок
    for link in element.find_all('a'):
        href = link.get('href', '')
        if href and not href.startswith('javascript:'):
            link.replace_with(f"[{link.get_text(strip=True)}]({href})")
    
    # Обработка жирного и курсива
    for tag in element.find_all(['b', 'strong']):
        tag.replace_with(f"**{tag.get_text(strip=True)}**")
    for tag in element.find_all(['i', 'em']):
        tag.replace_with(f"*{tag.get_text(strip=True)}*")
    
    # Обработка кода
    for tag in element.find_all('code'):
        tag.replace_with(f"`{tag.get_text(strip=True)}`")
    
    # Обработка переносов строк
    for br in element.find_all('br'):
        br.replace_with('\n')
    
    text = element.get_text(strip=False)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def parse_media_album(wrap: BeautifulSoup) -> List[Dict[str, Any]]:
    """Парсит альбомы (несколько фото/видео в одном посте)."""
    media = []
    album = wrap.find('div', class_='tgme_widget_message_album')
    if not album:
        return media
    
    for photo_wrap in album.find_all('a', class_='tgme_widget_message_photo_wrap'):
        style = photo_wrap.get('style', '')
        url = extract_bg_image(style)
        if url:
            media.append({
                'type': 'photo',
                'directUrl': url,
                'width': 800,
                'height': 600
            })
    return media

def parse_poll(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Парсит опросы."""
    poll = wrap.find('div', class_='tgme_widget_message_poll')
    if not poll:
        return None
    
    question = poll.find('div', class_='tgme_widget_message_poll_question')
    options = []
    for opt in poll.find_all('div', class_='tgme_widget_message_poll_option'):
        opt_text = opt.get_text(strip=True)
        percent = None
        percent_el = opt.find('div', class_='tgme_widget_message_poll_option_percent')
        if percent_el:
            try:
                percent = int(percent_el.get_text(strip=True).replace('%', ''))
            except:
                pass
        options.append({'text': opt_text, 'percent': percent})
    
    return {
        'type': 'poll',
        'question': question.get_text(strip=True) if question else '',
        'options': options
    }

def parse_voice_message(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Парсит голосовые сообщения."""
    audio = wrap.find('audio', class_='tgme_widget_message_voice')
    if not audio or not audio.get('src'):
        return None
    
    duration = None
    duration_el = wrap.find('time', class_='tgme_widget_message_voice_duration')
    if duration_el:
        duration_text = duration_el.get_text(strip=True)
        try:
            parts = duration_text.split(':')
            if len(parts) == 2:
                duration = int(parts[0]) * 60 + int(parts[1])
        except:
            pass
    
    return {
        'type': 'voice',
        'directUrl': audio['src'],
        'duration': duration
    }

def parse_document(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Парсит документы/файлы."""
    doc_link = wrap.find('a', class_='tgme_widget_message_document')
    if not doc_link:
        return None
    
    title_el = doc_link.find('div', class_='tgme_widget_message_document_title')
    size_el = doc_link.find('div', class_='tgme_widget_message_document_size')
    
    return {
        'type': 'document',
        'directUrl': doc_link.get('href', ''),
        'filename': title_el.get_text(strip=True) if title_el else 'Файл',
        'size': size_el.get_text(strip=True) if size_el else None
    }

def parse_video(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Парсит видео."""
    video_player = wrap.find('a', class_='tgme_widget_message_video_player')
    if not video_player:
        return None
    
    video_el = wrap.find('video', class_='tgme_widget_message_video')
    if not video_el:
        return None
    
    poster = video_el.get('poster', '')
    src = video_el.get('src') or video_el.find('source', {}).get('src', '')
    
    duration = None
    duration_el = wrap.find('time', class_='tgme_widget_message_video_duration')
    if duration_el:
        duration_text = duration_el.get_text(strip=True)
        try:
            parts = duration_text.split(':')
            if len(parts) == 2:
                duration = int(parts[0]) * 60 + int(parts[1])
        except:
            pass
    
    return {
        'type': 'video',
        'directUrl': src,
        'thumbnail': poster,
        'duration': duration,
        'width': 1280,
        'height': 720
    }

def parse_photo(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Парсит одиночное фото."""
    photo_wrap = wrap.find('a', class_='tgme_widget_message_photo_wrap')
    if not photo_wrap:
        return None
    
    style = photo_wrap.get('style', '')
    url = extract_bg_image(style)
    if not url:
        return None
    
    return {
        'type': 'photo',
        'directUrl': url,
        'width': 800,
        'height': 600
    }

def parse_repost_info(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Парсит информацию о репосте (форварде)."""
    forwarded = wrap.find('div', class_='tgme_widget_message_forwarded_from')
    if not forwarded:
        return None
    
    repost_info = {'isRepost': True}
    
    # Имя канала/пользователя
    name_el = forwarded.find('a', class_='tgme_widget_message_forwarded_from_name')
    if name_el:
        repost_info['repostFrom'] = {
            'name': name_el.get_text(strip=True),
            'username': None,
            'link': None
        }
        href = name_el.get('href', '')
        if href:
            # Извлекаем username из ссылки вида /s/username или /username
            match = re.search(r'/s?/([a-zA-Z0-9_]+)', href)
            if match:
                repost_info['repostFrom']['username'] = match.group(1)
                repost_info['repostFrom']['link'] = f"https://t.me/s/{match.group(1)}"
    
    # Ссылка на оригинальное сообщение (если есть)
    link_el = forwarded.find('a', href=True)
    if link_el and '/sochiautoparts/' not in link_el['href'] and 't.me/' in link_el['href']:
        repost_info['repostOriginalLink'] = link_el['href']
        if not repost_info['repostOriginalLink'].startswith('http'):
            repost_info['repostOriginalLink'] = f"https://t.me{link_el['href']}"
    
    return repost_info

def parse_message_metadata(message_div: BeautifulSoup) -> Dict[str, Any]:
    """Извлекает метаданные поста."""
    metadata = {}
    
    # ID поста
    data_post = message_div.get('data-post', '')
    if data_post and '/' in data_post:
        metadata['numericId'] = data_post.split('/')[-1]
    
    # Просмотры и репосты
    metadata['views'] = int(message_div.get('data-views', 0) or 0)
    metadata['forwards'] = int(message_div.get('data-forwards', 0) or 0)
    
    # Дата
    time_el = message_div.find('time', class_='tgme_widget_message_date')
    if time_el and time_el.get('datetime'):
        try:
            metadata['date'] = datetime.fromisoformat(time_el['datetime'].replace('Z', '+00:00'))
        except:
            metadata['date'] = datetime.now()
    else:
        metadata['date'] = datetime.now()
    
    return metadata

def fetch_page(url: str, page_num: int = 1) -> Optional[BeautifulSoup]:
    """Загружает страницу с обработкой ошибок."""
    headers = {
        'User-Agent': random.choice(CONFIG['USER_AGENTS']),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://t.me/',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    
    try:
        logger.info(f"[Page {page_num}] Загрузка: {url}")
        response = SESSION.get(url, headers=headers, timeout=CONFIG['TIMEOUT'])
        response.raise_for_status()
        
        if 'captcha' in response.text.lower() or response.status_code == 403:
            logger.warning(f"[Page {page_num}] Возможно, сработала защита. Статус: {response.status_code}")
            return None
        
        return BeautifulSoup(response.text, 'html.parser')
    except requests.exceptions.RequestException as e:
        logger.error(f"[Page {page_num}] Ошибка загрузки: {e}")
        return None

def parse_posts_from_page(soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
    """Парсит посты из одной страницы."""
    posts = []
    messages = soup.find_all('div', class_='tgme_widget_message', attrs={'data-post': True})
    
    for msg in messages:
        try:
            # Метаданные
            meta = parse_message_metadata(msg)
            if not meta.get('numericId'):
                continue
            
            # Текст поста
            text_el = msg.find('div', class_='tgme_widget_message_text')
            text = extract_text_with_formatting(text_el) if text_el else ''
            
            # Медиа
            media = []
            
            # 1. Альбомы (приоритет)
            album_media = parse_media_album(msg)
            if album_media:
                media.extend(album_media)
            else:
                # 2. Одиночное фото
                photo = parse_photo(msg)
                if photo:
                    media.append(photo)
                # 3. Видео
                video = parse_video(msg)
                if video:
                    media.append(video)
            
            # 4. Голосовые сообщения
            voice = parse_voice_message(msg)
            if voice:
                media.append(voice)
            
            # 5. Документы
            doc = parse_document(msg)
            if doc:
                media.append(doc)
            
            # 6. Опросы
            poll = parse_poll(msg)
            if poll:
                media.append(poll)
            
            # Репосты (форварды)
            repost_info = parse_repost_info(msg) or {}
            
            # Генерация заголовка
            title = text[:50] + '...' if len(text) > 50 else text
            
            # Ссылки
            post_id = meta['numericId']
            telegram_link = f"{base_url}/{post_id}"
            
            post = {
                'id': post_id,
                'numericId': post_id,
                'date': meta['date'].isoformat() if isinstance(meta['date'], datetime) else meta['date'],
                'text': text,
                'title': title,
                'media': media,
                'hasMedia': len(media) > 0,
                'mediaCount': len(media),
                'telegramLink': telegram_link,
                'views': meta.get('views', 0),
                'forwards': meta.get('forwards', 0),
                'keywords': [],
                'hashtags': [],
                # Поля для репостов (совместимы с Worker)
                'isRepost': repost_info.get('isRepost', False),
                'repostFrom': repost_info.get('repostFrom'),
                'repostOriginalLink': repost_info.get('repostOriginalLink')
            }
            
            posts.append(post)
            logger.debug(f"[Parsed] Пост #{post_id}: {len(media)} медиа, репост={post['isRepost']}")
            
        except Exception as e:
            logger.error(f"[Parse Error] Ошибка при парсинге сообщения: {e}")
            continue
    
    return posts

def get_next_page_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    """Находит ссылку на следующую страницу."""
    # Метод 1: Кнопка "Показать ещё"
    load_more = soup.find('a', class_='tme_messages_more')
    if load_more and load_more.get('href'):
        return urljoin(base_url, load_more['href'])
    
    # Метод 2: Ссылка с параметром ?before=
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '?before=' in href or '&before=' in href:
            return urljoin(base_url, href)
    
    return None

def generate_hashtags(text: str, keywords: List[str]) -> List[str]:
    """Генерирует хештеги из ключевых слов."""
    hashtags = []
    seen = set()
    
    for kw in keywords:
        if len(kw) < 2:
            continue
        tag = '#' + re.sub(r'[^\wа-яА-ЯёЁ0-9]', '', kw).lower()
        if tag not in seen and len(tag) <= 30:
            seen.add(tag)
            hashtags.append(tag)
    
    # Обязательные хештеги (без пробелов, как в Worker v84.1)
    mandatory = ['#автоновости', '#автомобили', '#авто', '#sochiautoparts']
    for m in mandatory:
        if m not in seen:
            hashtags.append(m)
    
    return hashtags[:15]

def extract_keywords(text: str) -> List[str]:
    """Извлекает ключевые слова из текста."""
    if not text:
        return []
    
    cleaned = re.sub(r'#[\wа-яА-ЯёЁ]+', '', text)
    cleaned = re.sub(r'@[\w]+', '', cleaned)
    cleaned = re.sub(r'https?://[^\s]+', '', cleaned)
    
    words = re.findall(r'[\wа-яА-ЯёЁ]{3,}', cleaned.lower())
    
    stop_words = {
        'это', 'как', 'так', 'что', 'все', 'они', 'был', 'была', 'было', 'были',
        'для', 'от', 'до', 'в', 'на', 'с', 'к', 'по', 'при', 'за', 'под', 'над',
        'и', 'или', 'но', 'а', 'же', 'ли', 'бы', 'то', 'тот', 'эта', 'это', 'эти',
        'который', 'какой', 'такой', 'сам', 'самый', 'весь', 'вся', 'всё', 'все',
        'свой', 'своя', 'своё', 'свои', 'мой', 'твой', 'наш', 'ваш',
        'the', 'and', 'for', 'are', 'but', 'not', 'you', 'all', 'can', 'had',
        'her', 'was', 'one', 'our', 'out', 'have', 'been', 'will', 'your',
        'its', 'from', 'they', 'this', 'that', 'with', 'just', 'what', 'when'
    }
    
    keywords = [w for w in words if w not in stop_words]
    return list(dict.fromkeys(keywords))[:20]

def main():
    """Основная функция парсинга."""
    logger.info("=== Запуск парсера SOCHIAUTOPARTS v2.2 ===")
    
    base_url = CONFIG['CHANNEL_URL']
    current_url = base_url
    all_posts = []
    media_map = {}
    page_num = 1
    
    while page_num <= CONFIG['MAX_PAGES'] and len(all_posts) < CONFIG['MAX_POSTS']:
        logger.info(f"=== Страница {page_num} ===")
        
        soup = fetch_page(current_url, page_num)
        if not soup:
            logger.warning(f"Не удалось загрузить страницу {page_num}, завершение")
            break
        
        posts = parse_posts_from_page(soup, base_url)
        if not posts:
            logger.warning(f"Посты не найдены на странице {page_num}")
            break
        
        for post in posts:
            # Ключевые слова и хештеги
            keywords = extract_keywords(post['text'])
            post['keywords'] = keywords
            post['hashtags'] = generate_hashtags(post['text'], keywords)
            
            # Добавление в медиа-мапу
            for m in post.get('media', []):
                if m.get('directUrl'):
                    url = m['directUrl']
                    media_hash = str(hash(url) & 0xFFFFFFFF)
                    media_map[media_hash] = url
            
            all_posts.append(post)
        
        logger.info(f"Загружено {len(posts)} постов. Всего: {len(all_posts)}")
        
        # Пагинация
        next_url = get_next_page_url(soup, base_url)
        if not next_url:
            logger.info("Следующая страница не найдена, завершение")
            break
        
        current_url = next_url
        page_num += 1
        
        # Рандомизированная задержка
        delay = random.uniform(*CONFIG['REQUEST_DELAY'])
        logger.info(f"Задержка {delay:.2f}с перед следующим запросом...")
        time.sleep(delay)
    
    # Сортировка по дате (новые сначала)
    all_posts.sort(key=lambda x: x['date'], reverse=True)
    
    # Ограничение по количеству
    if len(all_posts) > CONFIG['MAX_POSTS']:
        all_posts = all_posts[:CONFIG['MAX_POSTS']]
    
    # Сохранение результатов
    output_path = Path(CONFIG['OUTPUT_FILE'])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"Сохранено {len(all_posts)} постов в {CONFIG['OUTPUT_FILE']}")
    
    # Сохранение медиа-мапы
    media_path = Path(CONFIG['MEDIA_MAP_FILE'])
    with open(media_path, 'w', encoding='utf-8') as f:
        json.dump(media_map, f, ensure_ascii=False, indent=2)
    logger.info(f"Сохранена медиа-мапа ({len(media_map)} записей) в {CONFIG['MEDIA_MAP_FILE']}")
    
    # Статистика репостов
    repost_count = sum(1 for p in all_posts if p.get('isRepost'))
    logger.info(f"=== Парсинг завершён. Репостов: {repost_count}/{len(all_posts)} ===")
    
    return len(all_posts)

if __name__ == '__main__':
    try:
        count = main()
        print(f"\n✅ Готово! Загружено {count} постов.")
    except KeyboardInterrupt:
        logger.info("Прервано пользователем")
    except Exception as e:
        logger.exception(f"Критическая ошибка: {e}")
        exit(1)
