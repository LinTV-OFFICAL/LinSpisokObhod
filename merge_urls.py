#!/usr/bin/env python3
"""
Объединяет содержимое ресурсов из списка URL и разбивает на файлы.
Файлы сохраняются в указанной папке (по умолчанию config) по 1500 строк в каждом.
"""

import argparse
import sys
import requests
from pathlib import Path

def parse_args():
    parser = argparse.ArgumentParser(
        description="Скачать содержимое по ссылкам из файла, объединить и разбить на файлы по строкам"
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
        "--separator",
        default="--- [ {url} ] ---\n",
        help="Разделитель между содержимым разных URL. "
             "Может содержать {url} для подстановки адреса. "
             "Если указать пустую строку, разделитель не добавляется. "
             "По умолчанию: '--- [ {url} ] ---\\n'"
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

class FileWriter:
    """Управляет записью строк в файлы с автоматическим переключением при достижении лимита."""
    def __init__(self, output_dir, prefix, lines_per_file):
        self.output_dir = Path(output_dir)
        self.prefix = prefix
        self.lines_per_file = lines_per_file
        self.current_file_index = 1
        self.current_file = None
        self.line_count = 0
        self.total_lines_written = 0

    def _open_new_file(self):
        """Открывает новый файл для записи."""
        if self.current_file:
            self.current_file.close()
        filename = f"{self.prefix}{self.current_file_index:03d}.txt"
        filepath = self.output_dir / filename
        self.current_file = open(filepath, 'w', encoding='utf-8')
        self.line_count = 0
        print(f"📄 Создан файл: {filepath}", file=sys.stderr)

    def write_lines(self, lines):
        """Записывает список строк, разбивая на файлы при необходимости."""
        for line in lines:
            # Если нужно начать новый файл
            if self.current_file is None:
                self._open_new_file()
            elif self.line_count >= self.lines_per_file:
                self.current_file_index += 1
                self._open_new_file()

            self.current_file.write(line)
            # Гарантируем перевод строки, если его нет
            if not line.endswith('\n'):
                self.current_file.write('\n')
                line += '\n'  # для подсчёта строк считаем одну строку
            self.line_count += 1
            self.total_lines_written += 1

    def close(self):
        """Закрывает текущий файл, если он открыт."""
        if self.current_file:
            self.current_file.close()
            self.current_file = None

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

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    writer = FileWriter(output_dir, args.prefix, args.lines_per_file)

    success_count = 0
    error_count = 0

    try:
        for idx, url in enumerate(urls):
            print(f"📥 Обработка {url}", file=sys.stderr)
            content = download_content(url, args.timeout)
            if content is None:
                error_count += 1
                continue

            success_count += 1

            # Добавляем разделитель, если он не пустой и это не первый успешный блок
            if args.separator and success_count > 1:
                sep_line = args.separator.format(url=url)
                # Разделитель может содержать несколько строк, разбиваем
                sep_lines = sep_line.splitlines(keepends=True)
                writer.write_lines(sep_lines)

            # Разбиваем содержимое на строки и записываем
            content_lines = content.splitlines(keepends=True)
            writer.write_lines(content_lines)

    finally:
        writer.close()

    print(f"\n✅ Готово. Успешно обработано: {success_count}, пропущено: {error_count}", file=sys.stderr)
    print(f"📁 Всего строк записано: {writer.total_lines_written}", file=sys.stderr)
    print(f"📁 Файлы сохранены в папке {output_dir}", file=sys.stderr)

if __name__ == "__main__":
    main()
