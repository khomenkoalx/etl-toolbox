import sys
from pathlib import Path
from datetime import datetime
from etl.processor import DataProcessor
import shutil  # для перемещения файлов

def get_data_type_from_filename(filename):
    # ... (остаётся без изменений)
    name_without_ext = filename.split('.')[0].upper()
    data_types = ['ПРОДАЖИ ДБ', 'ВОЗВРАТЫ ДБ', 'ТРАНЗИТ ДБ', 'ОСТАТКИ ДБ', 'ПРОДАЖИ', 'ОСТАТКИ', 'ЗАКУПКИ']
    for data_type in data_types:
        if name_without_ext.startswith(data_type):
            return data_type
    return None

def format_metrics(metrics):
    # ... (остаётся без изменений)
    result = []
    result.append(f"📊 Всего строк: {metrics['total_rows']:,}")
    result.append(f"📦 Обработано чанков: {metrics['chunks_processed']}")
    
    if metrics['total_rows'] > 0:
        valid_percent = (metrics['valid_rows'] / metrics['total_rows']) * 100
        error_percent = (metrics['error_rows'] / metrics['total_rows']) * 100
    else:
        valid_percent = error_percent = 0
    
    result.append(f"✅ Валидных: {metrics['valid_rows']:,} ({valid_percent:.1f}%)")
    result.append(f"❌ Ошибок: {metrics['error_rows']:,} ({error_percent:.1f}%)")
    
    if metrics.get('error_types'):
        result.append(f"\n⚠️  ТИПЫ ОШИБОК:")
        for validator, count in metrics['error_types'].items():
            percent = (count / metrics['error_rows']) * 100 if metrics['error_rows'] > 0 else 0
            result.append(f"   • {validator}: {count:,} ({percent:.1f}%)")
    
    result.append(f"\n⏱️  Время обработки: {metrics['processing_time']:.2f} сек")
    if metrics['processing_time'] > 0:
        speed = metrics['total_rows'] / metrics['processing_time']
        result.append(f"📈 Скорость: {speed:.0f} строк/сек")
    
    return "\n".join(result)

def main():
    """
    Обработка всех CSV файлов в data/input по началу названия.
    После обработки — перемещает исходный файл в data/processed с временной меткой.
    """
    input_dir = Path('./data/input')
    output_dir = Path('./data/validated')
    processed_dir = Path('./data/processed')

    # Создаём все нужные директории
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("ОБРАБОТКА ФАЙЛОВ В ДИРЕКТОРИИ data/input")
    print("=" * 60)

    csv_files = list(input_dir.glob('*.csv'))

    if not csv_files:
        print("\n❌ CSV файлы не найдены в data/input/")
        return

    print(f"\n📁 Найдено CSV файлов: {len(csv_files)}")
    print("-" * 60)

    processor = DataProcessor()
    processed_count = 0

    for file_path in csv_files:
        filename = file_path.name
        data_type = get_data_type_from_filename(filename)

        if not data_type:
            print(f"❌ {filename}: Некорректное название (должно начинаться с: ЗАКУПКИ, ПРОДАЖИ, ОСТАТКИ, ОСТАТКИ ДБ)")
            continue

        try:
            print(f"\n📂 Обработка: {filename}")
            print(f"   Тип: {data_type}")

            valid_file, error_file, metrics = processor.process(
                data_type=data_type,
                input_file=file_path,
                chunk_size=10000,
                output_dir=str(output_dir)
            )

            if valid_file:
                print(f"\n✅ ВАЛИДНЫЕ ДАННЫЕ:")
                print(f"   📁 Файл: {valid_file}")

            if error_file:
                print(f"\n❌ ОШИБКИ:")
                print(f"   📁 Файл: {error_file}")

            print(f"\n📊 СТАТИСТИКА ОБРАБОТКИ:")
            print(format_metrics(metrics))

            # === Перемещаем исходный файл в processed/ с временной меткой ===
            target_path = processed_dir / filename

            shutil.move(str(file_path), str(target_path))
            print(f"   🗂️  Исходный файл перемещён в: {target_path}")

            processed_count += 1

        except Exception as e:
            print(f"   ❌ Ошибка обработки: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "=" * 60)
    print(f"ИТОГО: Обработано файлов {processed_count}/{len(csv_files)}")
    print("=" * 60)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Обработка прервана")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()