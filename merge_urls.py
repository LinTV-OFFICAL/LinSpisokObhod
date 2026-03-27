#!/usr/bin/env python3
"""
Объединяет содержимое ресурсов из списка URL в один файл.
Пропускает недоступные ссылки.
"""

import argparse
import sys
import requests
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(
        description="Скачать содержимое по ссылкам из файла и объединить в один файл"
    )
    parser.add_argument(
        "--input", "-i",
        default="urls.txt",
        help="Файл со списком URL (по одному на строку, пустые строки игнорируются). "
             "По умолчанию: urls.txt"
    )
    parser.add_argument(
        "--output", "-o",
        default="merged.txt",
        help="Итоговый файл с объединённым содержимым. По умолчанию: merged.txt"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Таймаут запроса в секундах. По умолчанию: 30"
    )
    parser.add_argument(
        "--separator",
        default="\n--- [ {url} ] ---\n",
        help="Разделитель между содержимым разных URL. "
             "Может содержать {url} для подстановки адреса. "
             "Если указать пустую строку, разделитель не добавляется. "
             "По умолчанию: '\\n--- [ {url} ] ---\\n'"
    )
    return parser.parse_args()

def read_urls(file_path):
    """Читает непустые строки из файла."""
    urls = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                urls.append(line)
    return urls

def download_content(url, timeout):
    """Скачивает содержимое URL. Возвращает текст или None при ошибке."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; MergeURLsBot/1.0)'}
        resp = requests.get(url, timeout=timeout, headers=headers)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.RequestException as e:
        print(f"⚠️  Ошибка при загрузке {url}: {e}", file=sys.stderr)
        return None
    except UnicodeDecodeError:
        print(f"⚠️  Не удалось декодировать содержимое {url} как UTF-8", file=sys.stderr)
        return None

def main():
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"❌ Файл {input_path} не найден", file=sys.stderr)
        sys.exit(1)

    urls = read_urls(input_path)
    if not urls:
        print("⚠️  Нет URL для обработки", file=sys.stderr)
        sys.exit(0)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    success_count = 0
    error_count = 0

    with open(output_path, 'w', encoding='utf-8') as out_f:
        for idx, url in enumerate(urls):
            print(f"📥 Обработка {url}", file=sys.stderr)   # убрали троеточие
            content = download_content(url, args.timeout)
            if content is None:
                error_count += 1
                continue

            success_count += 1
            # Добавляем разделитель, если он не пустой и это не первый успешный блок
            if args.separator and success_count > 1:
                separator = args.separator.format(url=url)
                out_f.write(separator)
            out_f.write(content)

    print(f"\n✅ Готово. Успешно обработано: {success_count}, пропущено: {error_count}", file=sys.stderr)
    print(f"📄 Результат сохранён в {output_path}", file=sys.stderr)

if __name__ == "__main__":
    main()
