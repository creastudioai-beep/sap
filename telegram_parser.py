#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SOCHIAUTOPARTS Telegram Parser v2.3 (Catch-Up + Reposts)
Совместим с Cloudflare Worker v84.1-AUDITED
"""

import re
import json
import time
import random
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ============================================================
# НАСТРОЙКИ
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler('parser.log', encoding='utf-8', mode='a'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

CONFIG = {
    'CHANNEL_URL': 'https://t.me/s/sochiautoparts',
    'OUTPUT_FILE': 'cached_posts.json',
    'MEDIA_MAP_FILE': 'media_map.json',
    'MAX_PAGES': 30,
    'MAX_POSTS': 2000,
    'REQUEST_DELAY': (0.3, 0.7),
    'MAX_RETRIES': 3,
    'RETRY_BACKOFF': 0.3,
    'TIMEOUT': 30,
    'USER_AGENTS': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0',
    ]
}

# Сессия с retry
def create_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=CONFIG['MAX_RETRIES'], backoff_factor=CONFIG['RETRY_BACKOFF'],
                  status_forcelist=[429, 500, 502, 503, 504], allowed_methods=['GET'])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

SESSION = create_session()

# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================
def extract_bg_image(style: Optional[str]) -> str:
    if not style: return ''
    match = re.search(r'url\(\s*["\']?([^"\')\s]+)["\']?\s*\)', style)
    return match.group(1).strip() if match else ''

def extract_text_with_formatting(element) -> str:
    if not element: return ''
    for link in element.find_all('a'):
        href = link.get('href', '')
        if href and not href.startswith('javascript:'):
            link.replace_with(f"[{link.get_text(strip=True)}]({href})")
    for tag in element.find_all(['b', 'strong']): tag.replace_with(f"**{tag.get_text(strip=True)}**")
    for tag in element.find_all(['i', 'em']): tag.replace_with(f"*{tag.get_text(strip=True)}*")
    for br in element.find_all('br'): br.replace_with('\n')
    text = re.sub(r'\n{3,}', '\n\n', element.get_text(strip=False)).strip()
    return text

def parse_media_album(wrap: BeautifulSoup) -> List[Dict[str, Any]]:
    media = []
    album = wrap.find('div', class_='tgme_widget_message_album')
    if not album: return media
    for photo_wrap in album.find_all('a', class_='tgme_widget_message_photo_wrap'):
        url = extract_bg_image(photo_wrap.get('style', ''))
        if url: media.append({'type': 'photo', 'directUrl': url, 'width': 800, 'height': 600})
    return media

def parse_poll(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    poll = wrap.find('div', class_='tgme_widget_message_poll')
    if not poll: return None
    question = poll.find('div', class_='tgme_widget_message_poll_question')
    options = []
    for opt in poll.find_all('div', class_='tgme_widget_message_poll_option'):
        pct = opt.find('div', class_='tgme_widget_message_poll_option_percent')
        options.append({
            'text': opt.get_text(strip=True),
            'percent': int(pct.get_text().replace('%', '')) if pct else None
        })
    return {
        'type': 'poll',
        'question': question.get_text(strip=True) if question else '',
        'options': options
    }

def parse_voice(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    audio = wrap.find('audio', class_='tgme_widget_message_voice')
    if not audio or not audio.get('src'): return None
    dur_el = wrap.find('time', class_='tgme_widget_message_voice_duration')
    duration = None
    if dur_el:
        try:
            m, s = map(int, dur_el.get_text().split(':'))
            duration = m * 60 + s
        except: pass
    return {'type': 'voice', 'directUrl': audio['src'], 'duration': duration}

def parse_document(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    doc = wrap.find('a', class_='tgme_widget_message_document')
    if not doc: return None
    return {
        'type': 'document',
        'directUrl': doc.get('href', ''),
        'filename': doc.find('div', class_='tgme_widget_message_document_title').get_text(strip=True) if doc.find('div', class_='tgme_widget_message_document_title') else 'Файл',
        'size': doc.find('div', class_='tgme_widget_message_document_size').get_text(strip=True) if doc.find('div', class_='tgme_widget_message_document_size') else None
    }

def parse_video(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    player = wrap.find('a', class_='tgme_widget_message_video_player')
    if not player: return None
    video = wrap.find('video', class_='tgme_widget_message_video')
    if not video: return None
    src = video.get('src') or (video.find('source') or {}).get('src', '')
    poster = video.get('poster', '')
    dur_el = wrap.find('time', class_='tgme_widget_message_video_duration')
    duration = None
    if dur_el:
        try: duration = int(dur_el.get_text().split(':')[0]) * 60 + int(dur_el.get_text().split(':')[1])
        except: pass
    return {'type': 'video', 'directUrl': src, 'thumbnail': poster, 'duration': duration, 'width': 1280, 'height': 720}

def parse_photo(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    wrap_el = wrap.find('a', class_='tgme_widget_message_photo_wrap')
    if not wrap_el: return None
    url = extract_bg_image(wrap_el.get('style', ''))
    return {'type': 'photo', 'directUrl': url, 'width': 800, 'height': 600} if url else None

def parse_repost_info(wrap: BeautifulSoup) -> Optional[Dict[str, Any]]:
    fwd = wrap.find('div', class_='tgme_widget_message_forwarded_from')
    if not fwd: return None
    info = {'isRepost': True}
    name_el = fwd.find('a', class_='tgme_widget_message_forwarded_from_name')
    if name_el:
        info['repostFrom'] = {'name': name_el.get_text(strip=True)}
        href = name_el.get('href', '')
        m = re.search(r'/s?/([a-zA-Z0-9_]+)', href)
        if m:
            info['repostFrom']['username'] = m.group(1)
            info['repostFrom']['link'] = f"https://t.me/s/{m.group(1)}"
    link_el = fwd.find('a', href=True)
    if link_el and 't.me/' in link_el['href']:
        info['repostOriginalLink'] = link_el['href'] if link_el['href'].startswith('http') else f"https://t.me{link_el['href']}"
    return info

def parse_metadata(msg: BeautifulSoup) -> Dict[str, Any]:
    meta = {'views': 0, 'forwards': 0, 'date': datetime.now()}
    dp = msg.get('data-post', '')
    meta['numericId'] = dp.split('/')[-1] if '/' in dp else ''
    try: meta['views'] = int(msg.get('data-views', 0))
    except: pass
    try: meta['forwards'] = int(msg.get('data-forwards', 0))
    except: pass
    time_el = msg.find('time', class_='tgme_widget_message_date')
    if time_el and time_el.get('datetime'):
        try: meta['date'] = datetime.fromisoformat(time_el['datetime'].replace('Z', '+00:00'))
        except: pass
    return meta

def fetch_page(url: str, page: int) -> Optional[BeautifulSoup]:
    try:
        logger.info(f"[Page {page}] Fetching {url}")
        resp = SESSION.get(url, headers={'User-Agent': random.choice(CONFIG['USER_AGENTS']), 'Accept': 'text/html'}, timeout=CONFIG['TIMEOUT'])
        resp.raise_for_status()
        if 'captcha' in resp.text.lower() or resp.status_code == 403:
            logger.warning(f"[Page {page}] CAPTCHA/Block detected")
            return None
        return BeautifulSoup(resp.text, 'html.parser')
    except Exception as e:
        logger.error(f"[Page {page}] Error: {e}")
        return None

def parse_posts_from_page(soup: BeautifulSoup, base_url: str) -> List[Dict[str, Any]]:
    posts = []
    for msg in soup.find_all('div', class_='tgme_widget_message', attrs={'data-post': True}):
        try:
            meta = parse_metadata(msg)
            if not meta.get('numericId'): continue

            text = extract_text_with_formatting(msg.find('div', class_='tgme_widget_message_text'))
            media = []

            # 1. Альбомы
            album = parse_media_album(msg)
            if album:
                media.extend(album)
            else:
                # 2. Одиночное фото
                p = parse_photo(msg)
                if p: media.append(p)
                # 3. Видео
                v = parse_video(msg)
                if v: media.append(v)

            # 4. Голосовые
            vo = parse_voice(msg)
            if vo: media.append(vo)
            # 5. Документы
            d = parse_document(msg)
            if d: media.append(d)
            # 6. Опросы
            poll = parse_poll(msg)
            if poll: media.append(poll)

            repost = parse_repost_info(msg) or {}
            pid = meta['numericId']

            posts.append({
                'id': pid,
                'numericId': pid,
                'date': meta['date'].isoformat(),
                'text': text,
                'title': (text[:50] + '...') if len(text) > 50 else text,
                'media': media,
                'hasMedia': len(media) > 0,
                'mediaCount': len(media),
                'telegramLink': f"{base_url}/{pid}",
                'views': meta['views'],
                'forwards': meta['forwards'],
                'keywords': [],
                'hashtags': [],
                'isRepost': repost.get('isRepost', False),
                'repostFrom': repost.get('repostFrom'),
                'repostOriginalLink': repost.get('repostOriginalLink')
            })
        except Exception as e:
            logger.error(f"[Parse] Error: {e}")
    return posts

def get_next_page_url(soup: BeautifulSoup, base_url: str) -> Optional[str]:
    btn = soup.find('a', class_='tme_messages_more')
    if btn and btn.get('href'): return urljoin(base_url, btn['href'])
    for a in soup.find_all('a', href=True):
        if '?before=' in a['href']: return urljoin(base_url, a['href'])
    return None

def extract_keywords(text: str) -> List[str]:
    if not text: return []
    cleaned = re.sub(r'#[\wа-яА-ЯёЁ]+|@\w+|https?://\S+', '', text).lower()
    words = re.findall(r'[а-яёa-z0-9]{3,}', cleaned)
    stop = {'и','в','во','не','что','он','на','я','с','со','как','а','то','все','она','так','его','но','да','ты','к','у','уже','вы','за','бы','по','только','её','мне','было','вот','от','the','and','for','are','but','not','you','all','can','had','her','was','one','our','out','have','been','will','your','its','from','they','this','that','with','just','what','when'}
    return list(dict.fromkeys(w for w in words if w not in stop))[:20]

def generate_hashtags(keywords: List[str]) -> List[str]:
    tags = [f'#{k.lower()}' for k in keywords if len(k) >= 2]
    mandatory = ['#автоновости', '#автомобили', '#авто', '#sochiautoparts']
    for m in mandatory:
        if m not in tags: tags.append(m)
    return tags[:15]

def merge_and_save_posts(new_posts: List[Dict], existing_posts: List[Dict]) -> List[Dict]:
    seen = {str(p['id']): p for p in existing_posts}
    for p in new_posts:
        seen[str(p['id'])] = p
    merged = list(seen.values())
    merged.sort(key=lambda x: x['date'], reverse=True)
    return merged[:CONFIG['MAX_POSTS']]

# ============================================================
# ГЛАВНАЯ ЛОГИКА
# ============================================================
def main():
    logger.info("=== SOCHIAUTOPARTS Parser v2.3 (Catch-Up + Reposts) ===")
    base_url = CONFIG['CHANNEL_URL']
    
    # Загрузка существующего кеша для catch-up логики
    existing_posts = []
    existing_ids: Set[str] = set()
    if Path(CONFIG['OUTPUT_FILE']).exists():
        try:
            with open(CONFIG['OUTPUT_FILE'], 'r', encoding='utf-8') as f:
                existing_posts = json.load(f)
            existing_ids = {str(p['id']) for p in existing_posts if isinstance(p, dict) and 'id' in p}
            logger.info(f"[Cache] Loaded {len(existing_posts)} posts, {len(existing_ids)} unique IDs")
        except Exception as e:
            logger.warning(f"[Cache] Failed to load: {e}")

    new_posts: List[Dict] = []
    current_url = base_url
    page = 1
    caught_up = False
    media_map = {}

    while page <= CONFIG['MAX_PAGES'] and len(new_posts) < CONFIG['MAX_POSTS']:
        soup = fetch_page(current_url, page)
        if not soup: break

        posts = parse_posts_from_page(soup, base_url)
        if not posts:
            logger.info("[Parse] No posts found on page, stopping")
            break

        page_new = []
        for p in posts:
            # Catch-up: если пост уже в кеше, останавливаемся
            if str(p['id']) in existing_ids:
                caught_up = True
                logger.info(f"[Catch-Up] Hit known post boundary at page {page}")
                break

            # Ключевые слова и хештеги
            kw = extract_keywords(p['text'])
            p['keywords'] = kw
            p['hashtags'] = generate_hashtags(kw)

            # Медиа-мапа
            for m in p.get('media', []):
                if m.get('directUrl'):
                    h = str(hash(m['directUrl']) & 0xFFFFFFFF)
                    media_map[h] = m['directUrl']

            page_new.append(p)

        new_posts.extend(page_new)
        if caught_up: break

        next_url = get_next_page_url(soup, base_url)
        if not next_url:
            logger.info("[Pagination] No 'next' link found")
            break
        current_url = next_url
        page += 1
        time.sleep(random.uniform(*CONFIG['REQUEST_DELAY']))

    # Слияние и сохранение
    final_posts = merge_and_save_posts(new_posts, existing_posts)
    Path(CONFIG['OUTPUT_FILE']).parent.mkdir(parents=True, exist_ok=True)
    with open(CONFIG['OUTPUT_FILE'], 'w', encoding='utf-8') as f:
        json.dump(final_posts, f, ensure_ascii=False, indent=2, default=str)

    with open(CONFIG['MEDIA_MAP_FILE'], 'w', encoding='utf-8') as f:
        json.dump(media_map, f, ensure_ascii=False, indent=2)

    logger.info(f"✅ Done: {len(new_posts)} new, {len(final_posts)} total, {len(media_map)} media mapped")
    return len(new_posts)

if __name__ == '__main__':
    try:
        count = main()
        print(f"\n🚀 Готово! Загружено {count} новых постов.")
    except KeyboardInterrupt:
        logger.info("Aborted by user")
    except Exception as e:
        logger.exception(f"Critical error: {e}")
        exit(1)
