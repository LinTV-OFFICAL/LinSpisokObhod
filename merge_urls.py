#!/usr/bin/env python3
"""
Асинхронная загрузка и объединение содержимого из списка URL.
Разбивает результат на файлы по количеству строк.
"""

import argparse
import sys
import asyncio
import aiohttp
from pathlib import Path
from typing import List, Optional

def parse_args():
    parser = argparse.ArgumentParser(
        description="Асинхронно загрузить содержимое по ссылкам из файла, объединить и разбить на файлы по строкам"
    )
    parser.add_argument(
        "--input", "-i",
        default="urls.txt",
        help="Файл со списком URL (по одному на строку, пустые строки игнорируются). По умолчанию: urls.txt"
    )
    parser.add_argument(
        "--output-dir", "-o",
        default="config",
        help="Папка для сохранения итоговых файлов. По умолчанию: config"
    )
    parser.add_argument(
        "--lines-per-file", "-l",
        type=int,
        default=1500,
        help="Максимальное количество строк в одном файле. По умолчанию: 1500"
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
        default=10,
        help="Количество одновременных запросов. По умолчанию: 10"
    )
    parser.add_argument(
        "--separator",
        default="--- [ {url} ] ---\n",
        help="Разделитель между содержимым разных URL. "
             "Может содержать {url} для подстановки адреса. "
             "Если указать пустую строку, разделитель не добавляется. "
             "По умолчанию: '--- [ {url} ] ---\\n'"
    )
    return parser.parse_args()

def read_urls(file_path: Path) -> List[str]:
    """Читает непустые строки из файла."""
    urls = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                urls.append(line)
    return urls

async def download_content(session: aiohttp.ClientSession, url: str, timeout: int) -> Optional[str]:
    """Асинхронно скачивает содержимое URL. Возвращает текст или None при ошибке."""
    headers = {'User-Agent': 'Mozilla/5.0 (compatible; MergeURLsBot/1.0)'}
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout), headers=headers) as resp:
            resp.raise_for_status()
            # Используем text() с автоопределением кодировки (по умолчанию utf-8)
            return await resp.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"⚠️  Ошибка при загрузке {url}: {e}", file=sys.stderr)
        return None
    except UnicodeDecodeError:
        print(f"⚠️  Не удалось декодировать содержимое {url} как UTF-8", file=sys.stderr)
        return None

async def process_urls(urls: List[str], concurrency: int, timeout: int, separator_template: str) -> List[Optional[str]]:
    """Асинхронно загружает все URL, возвращает список содержимого в исходном порядке."""
    semaphore = asyncio.Semaphore(concurrency)

    async def fetch_with_semaphore(session, url):
        async with semaphore:
            return await download_content(session, url, timeout)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_with_semaphore(session, url) for url in urls]
        return await asyncio.gather(*tasks)

def write_files(lines: List[str], output_dir: Path, prefix: str, lines_per_file: int):
    """Записывает строки в файлы, разбивая по количеству строк."""
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
            f.writelines(chunk)  # writelines быстрее, чем построчная запись
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
    contents = asyncio.run(process_urls(urls, args.concurrency, args.timeout, args.separator))

    # Формируем общий список строк в исходном порядке, добавляя разделители
    all_lines = []
    first_success = True
    success_count = 0
    error_count = 0

    for url, content in zip(urls, contents):
        if content is None:
            error_count += 1
            continue
        success_count += 1
        if not first_success and args.separator:
            # Добавляем разделитель как отдельные строки
            sep = args.separator.format(url=url)
            all_lines.extend(sep.splitlines(keepends=True))
        first_success = False
        # Добавляем строки содержимого (сохраняем переводы строк)
        all_lines.extend(content.splitlines(keepends=True))

    print(f"✅ Загрузка завершена. Успешно: {success_count}, пропущено: {error_count}", file=sys.stderr)

    if all_lines:
        write_files(all_lines, Path(args.output_dir), args.prefix, args.lines_per_file)
    else:
        print("⚠️  Нет успешно загруженного содержимого для записи", file=sys.stderr)

if __name__ == "__main__":
    main()
