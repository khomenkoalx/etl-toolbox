"""
Универсальный процессор данных с прогресс-баром по чанкам и записью по мере обработки
"""
import csv
import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
import logging
from .validators import get_validator
from .transformers import get_transformer

# Настройка логгирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, config_path='etl/configs/etl_config.json'):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            logger.info(f"Загружен конфиг из {config_path}")
        except Exception as e:
            logger.error(f"Ошибка загрузки конфига: {e}")
            raise
    
    def get_config(self, data_type):
        """Получить конфигурацию для типа данных"""
        if data_type not in self.config:
            error_msg = f"Конфиг для '{data_type}' не найден"
            logger.error(error_msg)
            raise ValueError(error_msg)
        return self.config[data_type]
    
    def apply_transformations(self, df, transformation_rules, original_columns):
        """
        Применить трансформации к данным
        
        Args:
            df: DataFrame с данными
            transformation_rules: список правил трансформации
            original_columns: список оригинальных колонок для сохранения в ошибках
        
        Returns:
            DataFrame с примененными трансформациями
        """
        # Сохраняем оригинальные значения для колонок, которые могут трансформироваться
        for col in original_columns:
            if col in df.columns:
                df[f'{col}'] = df[col]
        
        # Применяем трансформации
        for rule in transformation_rules:
            try:
                transformer = get_transformer(rule['transformer'])
                input_columns = rule['input_columns']  # Может быть строкой или списком
                output_column = rule['output_column']

                # Если input_columns - строка, преобразуем в список
                if isinstance(input_columns, str):
                    input_columns = [input_columns]

                # Проверяем, что все входные колонки существуют
                missing_columns = [col for col in input_columns if col not in df.columns]
                if missing_columns:
                    logger.warning(f"Колонки {missing_columns} отсутствуют, пропускаем трансформацию {rule['transformer']}")
                    continue

                # Применяем трансформатор
                df = transformer(df, input_columns, output_column)

            except Exception as e:
                logger.error(f"Ошибка в трансформации {rule}: {e}")
                raise

        return df

    def validate_data(self, chunk, validation_rules):
        """
        Валидация данных

        Args:
            chunk: DataFrame с данными
            validation_rules: список правил валидации

        Returns:
            tuple: (error_mask, error_info)
                error_mask: булев массив с ошибками
                error_info: список словарей с информацией об ошибках
        """
        error_mask = pd.Series(False, index=chunk.index)
        error_info = []

        for rule in validation_rules:
            try:
                validator = get_validator(rule['validator'])
                column = rule['column']

                # Если колонка отсутствует, пропускаем валидацию
                if column not in chunk.columns:
                    logger.warning(f"Колонка '{column}' отсутствует, пропускаем валидацию {rule['validator']}")
                    continue

                # Выполняем валидацию
                rule_mask = validator(chunk, column)

                # Проверяем тип результата
                if not isinstance(rule_mask, pd.Series):
                    logger.warning(f"Валидатор {rule['validator']} вернул не pd.Series")
                    continue
                
                # Сохраняем информацию об ошибках
                if rule_mask.any():
                    error_rows = chunk[rule_mask]
                    for idx, row in error_rows.iterrows():
                        error_info.append({
                            'index': idx,
                            'validator': rule['validator'],
                            'column': column,
                            'error_value': row[column]
                        })
                    
                    error_mask = error_mask | rule_mask
                    
            except Exception as e:
                logger.error(f"Ошибка в валидаторе {rule}: {e}")
                raise
        
        return error_mask, error_info
    
    def get_output_columns(self, config):
        """Получить список выходных колонок для валидных данных"""
        base_columns = list(config['column_mapping'].values())
        
        # Добавляем выходные колонки из трансформаций
        for rule in config.get('transformation_rules', []):
            output_column = rule['output_column']
            if output_column not in base_columns:
                base_columns.append(output_column)
        
        return base_columns
    
    def get_error_columns(self, config):
        """Получить список колонок для отчета об ошибках"""
        base_columns = self.get_output_columns(config)
        error_columns = ['validator', 'column', 'error_value']
        
        # Добавляем оригинальные колонки из конфига
        original_columns = []
        for col in config.get('additional_columns_for_error_report', []):
            original_columns.append(f'{col}')
        
        all_columns = base_columns + error_columns + original_columns
        return all_columns
    
    def process(self, data_type, input_file, chunk_size=10000, output_dir='./data/validated', error_dir='./data/errors'):
        """
        Обработка данных с записью по чанкам
        
        Args:
            data_type: тип данных (ключ в конфиге)
            input_file: путь к входному файлу
            chunk_size: размер чанка
            output_dir: директория для выходных файлов
        
        Returns:
            tuple: (valid_file_path, error_file_path, metrics)
        """
        config = self.get_config(data_type)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        error_dir = Path(error_dir)
        error_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Начинаю обработку: {data_type}")
        logger.info(f"Входной файл: {input_file}")
        logger.info(f"Размер чанка: {chunk_size}")
        
        # Подготовка метрик
        metrics = {
            'total_rows': 0,
            'valid_rows': 0,
            'error_rows': 0,
            'start_time': datetime.now(),
            'chunks_processed': 0
        }
        
        # Определяем колонки для выходных файлов
        output_columns = self.get_output_columns(config)
        error_columns = self.get_error_columns(config)
        
        # Списки для сбора дат и ошибок
        all_dates = []
        error_types = {}
        
        # Подготовка путей к выходным файлам
        valid_file = None
        error_file = None
        
        # Флаг для первой записи
        first_valid_chunk = True
        first_error_chunk = True
        
        try:
            # Получаем приблизительное количество строк для прогресс-бара
            try:
                # Быстрая оценка количества строк
                with open(input_file, 'r', encoding='cp1251') as f:
                    line_count = sum(1 for _ in f)
                approx_chunks = max(1, line_count // chunk_size)
            except:
                approx_chunks = None
            
            # Создаем итератор для чтения файла
            chunks = pd.read_csv(
                input_file, 
                chunksize=chunk_size, 
                encoding='cp1251', 
                sep='\t', 
                low_memory=False,
                dtype=str
            )
            
            # Прогресс-бар
            with tqdm(total=approx_chunks, desc="Обработка чанков", unit="чанк", disable=approx_chunks is None) as pbar:
                for chunk in chunks:
                    metrics['chunks_processed'] += 1
                    metrics['total_rows'] += len(chunk)

                    # 1. Переименование колонок
                    chunk = chunk.rename(columns=config['column_mapping'])
                    
                    # 2. Собираем даты для именования файлов
                    if 'operation_date' in chunk.columns:
                        all_dates.extend(chunk['operation_date'].dropna().tolist())

                    # 3. Определяем колонки, которые нужны для отчета об ошибках
                    original_error_columns = config.get('additional_columns_for_error_report', [])
                    
                    # 4. Применяем трансформации
                    if 'transformation_rules' in config:
                        chunk = self.apply_transformations(
                            chunk, 
                            config['transformation_rules'],
                            original_error_columns
                        )
                    
                    # 5. Валидация данных
                    error_mask, error_info = self.validate_data(
                        chunk, 
                        config.get('validation_rules', [])
                    )
                    
                    # 6. Разделяем на валидные и ошибочные данные
                    valid_chunk = chunk[~error_mask].copy()
                    error_chunk = chunk[error_mask].copy()
                    
                    # 7. Добавляем информацию об ошибках в error_chunk
                    if not error_chunk.empty and error_info:
                        # Создаем маппинг индексов к ошибкам
                        error_dict = {}
                        for info in error_info:
                            idx = info['index']
                            if idx not in error_dict:
                                error_dict[idx] = []
                            error_dict[idx].append({
                                'validator': info['validator'],
                                'column': info['column'],
                                'error_value': info['error_value']
                            })
                        
                        # Для строк с несколькими ошибками объединяем информацию
                        for idx, errors in error_dict.items():
                            if idx in error_chunk.index:
                                # Берем первую ошибку (можно изменить логику)
                                error_chunk.loc[idx, 'validator'] = errors[0]['validator']
                                error_chunk.loc[idx, 'column'] = errors[0]['column']
                                error_chunk.loc[idx, 'error_value'] = errors[0]['error_value']
                                
                                # Считаем типы ошибок для метрик
                                for error in errors:
                                    validator_name = error['validator']
                                    error_types[validator_name] = error_types.get(validator_name, 0) + 1
                    
                    # 8. Записываем валидные данные
                    if not valid_chunk.empty:
                        metrics['valid_rows'] += len(valid_chunk)
                        
                        # Отфильтровываем только нужные колонки
                        existing_columns = [col for col in output_columns if col in valid_chunk.columns]
                        valid_chunk_filtered = valid_chunk[existing_columns]
                        
                        # Создаем или дописываем файл
                        if first_valid_chunk:
                            # Определяем имя файла
                            date_range = self._get_date_range(all_dates)
                            filename = input_file.name
                            valid_file = output_dir / filename
                            # Записываем с заголовком
                            valid_chunk_filtered.to_csv(
                                valid_file, 
                                mode='w', 
                                index=False,
                                sep=';',
                                decimal=',',
                                date_format='%Y-%m-%d'
                            )
                            first_valid_chunk = False
                        else:
                            # Дописываем без заголовка
                            valid_chunk_filtered.to_csv(
                                valid_file,
                                mode='a',
                                index=False,
                                header=False,
                                sep=';',
                                decimal=',',
                                date_format='%Y-%m-%d'
                            )
                    
                    # 9. Записываем ошибочные данные
                    if not error_chunk.empty:
                        metrics['error_rows'] += len(error_chunk)
                        
                        # Отфильтровываем только нужные колонки
                        existing_error_columns = [col for col in error_columns if col in error_chunk.columns]
                        error_chunk_filtered = error_chunk[existing_error_columns]
                        
                        # Создаем или дописываем файл
                        if first_error_chunk:
                            # Определяем имя файла
                            date_range = self._get_date_range(all_dates)
                            filename = f"{input_file.stem}_errors.csv"
                            error_file = error_dir / filename
                            
                            # Записываем с заголовком
                            error_chunk_filtered.to_csv(
                                error_file, 
                                mode='w', 
                                index=False, 
                                encoding='cp1251', 
                                sep=';'
                            )
                            first_error_chunk = False
                        else:
                            # Дописываем без заголовка
                            error_chunk_filtered.to_csv(
                                error_file, 
                                mode='a', 
                                index=False, 
                                header=False, 
                                encoding='cp1251', 
                                sep=';'
                            )
                    
                    # 10. Обновляем прогресс-бар
                    pbar.update(1)
                    pbar.set_postfix({
                        'строк': f"{metrics['total_rows']:,}",
                        'валид': f"{metrics['valid_rows']:,}",
                        'ошиб': f"{metrics['error_rows']:,}"
                    })
            
            # 11. Завершаем сбор метрик
            metrics['end_time'] = datetime.now()
            metrics['processing_time'] = (metrics['end_time'] - metrics['start_time']).total_seconds()
            metrics['error_types'] = error_types
            
            # 12. Выводим статистику
            self._print_statistics(metrics, valid_file, error_file)
            
            return valid_file, error_file, metrics
            
        except Exception as e:
            logger.error(f"Ошибка обработки: {e}")
            raise
    
    def _get_date_range(self, dates):
        """Получить диапазон дат в формате YYYY-MM-DD_YYYY-MM-DD"""
        if not dates:
            return None
        
        try:
            dates_series = pd.to_datetime(pd.Series(dates), errors='coerce')
            valid_dates = dates_series.dropna()
            
            if not valid_dates.empty:
                min_date_raw = valid_dates.min()
                
                # Устанавливаем min_date на первое число месяца
                min_date = min_date_raw.to_period('M').to_timestamp().strftime('%Y_%m_%d')
                
                return f"{min_date}"
        except Exception as e:
            logger.warning(f"Не удалось определить диапазон дат: {e}")
        
        return None
    
    def _print_statistics(self, metrics, valid_file, error_file):
        """Вывести статистику обработки"""
        print("\n" + "="*60)
        print("📈 ИТОГОВАЯ СТАТИСТИКА:")
        print("="*60)
        
        print(f"📊 Всего строк: {metrics['total_rows']:,}")
        print(f"📦 Обработано чанков: {metrics['chunks_processed']}")
        
        if metrics['total_rows'] > 0:
            valid_percent = (metrics['valid_rows'] / metrics['total_rows']) * 100
            error_percent = (metrics['error_rows'] / metrics['total_rows']) * 100
        else:
            valid_percent = error_percent = 0
        
        print(f"✅ Валидных: {metrics['valid_rows']:,} ({valid_percent:.1f}%)")
        print(f"❌ Ошибок: {metrics['error_rows']:,} ({error_percent:.1f}%)")
        
        if valid_file:
            print(f"\n✅ ВАЛИДНЫЕ ДАННЫЕ:")
            print(f"   📁 Файл: {valid_file}")
        
        if error_file:
            print(f"\n❌ ОШИБКИ:")
            print(f"   📁 Файл: {error_file}")
            
            if metrics.get('error_types'):
                print(f"\n⚠️  ТИПЫ ОШИБОК:")
                for validator, count in metrics['error_types'].items():
                    percent = (count / metrics['error_rows']) * 100 if metrics['error_rows'] > 0 else 0
                    print(f"   • {validator}: {count:,} ({percent:.1f}%)")
        
        print(f"\n⏱️  Время обработки: {metrics['processing_time']:.2f} сек")
        print(f"📈 Скорость: {metrics['total_rows'] / metrics['processing_time']:.0f} строк/сек" if metrics['processing_time'] > 0 else "")
        print("="*60)
