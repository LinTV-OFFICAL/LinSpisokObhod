#!/usr/bin/env python3
"""
Асинхронно загружает файлы по ссылкам, извлекает VPN-конфиги (ss://, vless://, vmess://, trojan://, hysteria2://)
и разбивает их на файлы по 5000 строк в папке config.
"""

import argparse
import sys
import asyncio
import aiohttp
from pathlib import Path
from typing import List, Optional, Set

def parse_args():
    parser = argparse.ArgumentParser(
        description="Загружает файлы по ссылкам, извлекает VPN-конфиги и разбивает на файлы"
    )
    parser.add_argument(
        "--input", "-i",
        default="urls.txt",
        help="Файл со списком URL (по одному на строку). По умолчанию: urls.txt"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default="config",
        help="Папка для сохранения итоговых файлов. По умолчанию: config"
    )
    parser.add_argument(
        "--lines-per-file", "-l",
        type=int,
        default=5000,
        help="Максимальное количество строк в одном файле. По умолчанию: 5000"
    )
    parser.add_argument(
        "--prefix",
        default="merged_",
        help="Префикс имён файлов. По умолчанию: merged_"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Таймаут запроса в секундах. По умолчанию: 30"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=30,
        help="Количество одновременных запросов. По умолчанию: 30"
    )
    parser.add_argument(
        "--clean-urls",
        action="store_true",
        help="Удалить из входного файла ссылки, которые не дали ни одного конфига. "
             "Нерабочие ссылки сохраняются в broken_urls.txt"
    )
    return parser.parse_args()

# Допустимые префиксы VPN-конфигов
VPN_PREFIXES: Set[str] = {
    "ss://",
    "vless://",
    "vmess://",
    "trojan://",
    "hysteria2://",
}

def read_urls(file_path: Path) -> List[str]:
    """Читает непустые строки из файла."""
    urls = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                urls.append(line)
    return urls

def write_urls(file_path: Path, urls: List[str]):
    """Записывает список URL в файл (по одному на строку)."""
    with open(file_path, 'w', encoding='utf-8') as f:
        for url in urls:
            f.write(url + '\n')

def extract_vpn_lines(text: str) -> List[str]:
    """Извлекает из текста строки, начинающиеся с VPN-префиксов."""
    lines = []
    for line in text.splitlines():
        line = line.strip()
        if any(line.startswith(prefix) for prefix in VPN_PREFIXES):
            lines.append(line)
    return lines

async def fetch_and_extract(session: aiohttp.ClientSession, url: str, timeout: int) -> Optional[List[str]]:
    """Загружает URL и возвращает список найденных VPN-строк или None при ошибке."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; VPNConfigsBot/1.0)'}
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), headers=headers) as resp:
            resp.raise_for_status()
            content = await resp.text()
            vpn_lines = extract_vpn_lines(content)
            if vpn_lines:
                return vpn_lines
            else:
                print(f"⚠️  В {url} не найдено VPN-строк", file=sys.stderr)
                return None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"⚠️  Ошибка при загрузке {url} — {e}", file=sys.stderr)
        return None
    except UnicodeDecodeError:
        print(f"⚠️  Не удалось декодировать {url} как UTF-8", file=sys.stderr)
        return None

async def process_urls(urls: List[str], concurrency: int, timeout: int) -> List[Optional[List[str]]]:
    """Асинхронно обрабатывает URL, возвращает списки найденных строк (в исходном порядке)."""
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_with_semaphore(session, url):
        async with semaphore:
            return await fetch_and_extract(session, url, timeout)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_semaphore(session, url) for url in urls]
        return await asyncio.gather(*tasks)

def write_split_files(lines: List[str], output_dir: Path, prefix: str, lines_per_file: int):
    """Разбивает строки на файлы и записывает."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if not lines:
        print("⚠️  Нет строк для записи", file=sys.stderr)
        return

    file_index = 1
    total_written = 0
    for i in range(0, len(lines), lines_per_file):
        chunk = lines[i:i + lines_per_file]
        filename = f"{prefix}{file_index:03d}.txt"
        filepath = output_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write('\n'.join(chunk))
            if chunk:
                f.write('\n')
        total_written += len(chunk)
        print(f"📄 Записан файл: {filepath} ({len(chunk)} строк)", file=sys.stderr)
        file_index += 1
    print(f"📁 Всего записано строк: {total_written}", file=sys.stderr)

def main():
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ Файл {input_path} не найден", file=sys.stderr)
        sys.exit(1)

    urls = read_urls(input_path)
    if not urls:
        print("⚠️  Нет URL для обработки", file=sys.stderr)
        return

    print(f"📥 Начинаем асинхронную загрузку {len(urls)} URL (параллелизм: {args.concurrency})...", file=sys.stderr)
    results = asyncio.run(process_urls(urls, args.concurrency, args.timeout))

    # Собираем все VPN-строки и отслеживаем рабочие/нерабочие URL
    all_vpn_lines = []
    working_urls = []
    broken_urls = []

    for url, vpn_list in zip(urls, results):
        if vpn_list is not None:
            working_urls.append(url)
            all_vpn_lines.extend(vpn_list)
        else:
            broken_urls.append(url)

    print(f"✅ Обработка завершена. Успешно: {len(working_urls)}, пропущено: {len(broken_urls)}", file=sys.stderr)

    # Сохраняем нерабочие URL
    if broken_urls:
        broken_path = input_path.parent / "broken_urls.txt"
        write_urls(broken_path, broken_urls)
        print(f"📝 Нерабочие ссылки сохранены в {broken_path}", file=sys.stderr)

    # Опционально очищаем входной файл
    if args.clean_urls:
        write_urls(input_path, working_urls)
        print(f"🧹 Входной файл {input_path} очищен (оставлено {len(working_urls)} ссылок)", file=sys.stderr)

    # Записываем найденные строки
    if all_vpn_lines:
        write_split_files(all_vpn_lines, Path(args.output_dir), args.prefix, args.lines_per_file)
    else:
        print("⚠️  Не найдено ни одной VPN-строки", file=sys.stderr)

if __name__ == "__main__":
    main()
