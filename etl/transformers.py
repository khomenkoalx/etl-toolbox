"""
Универсальные функции трансформации данных
"""
import pandas as pd
from core.crud import raw_pharmacy_crud

# ============ УНИВЕРСАЛЬНЫЕ ТРАНСФОРМЕРЫ ============

def set_to_zero(df, input_columns, output_column):
    """Установить значение 0 в указанную колонку"""
    if isinstance(input_columns, list) and len(input_columns) > 0:
        # Берем первую колонку для копирования типа
        df[output_column] = 0
    else:
        df[output_column] = 0
    return df


def set_to_two(df, input_columns, output_column):
    """Установить значение 2 в указанную колонку"""
    if isinstance(input_columns, list) and len(input_columns) > 0:
        # Берем первую колонку для копирования типа
        df[output_column] = 2
    else:
        df[output_column] = 2
    return df


def set_to_three(df, input_columns, output_column):
    """Установить значение 3 в указанную колонку"""
    if isinstance(input_columns, list) and len(input_columns) > 0:
        # Берем первую колонку для копирования типа
        df[output_column] = 3
    else:
        df[output_column] = 3
    return df


def set_to_four(df, input_columns, output_column):
    """Установить значение 4 в указанную колонку"""
    if isinstance(input_columns, list) and len(input_columns) > 0:
        # Берем первую колонку для копирования типа
        df[output_column] = 4
    else:
        df[output_column] = 4
    return df


def set_to_five(df, input_columns, output_column):
    """Установить значение 5 в указанную колонку"""
    if isinstance(input_columns, list) and len(input_columns) > 0:
        # Берем первую колонку для копирования типа
        df[output_column] = 5
    else:
        df[output_column] = 5
    return df


def set_to_minus_one(df, input_columns, output_column):
    """Установить значение -1 в указанную колонку"""
    if isinstance(input_columns, list) and len(input_columns) > 0:
        # Берем первую колонку для копирования типа
        df[output_column] = -1
    else:
        df[output_column] = -1
    return df

def copy_column(df, input_columns, output_column):
    """Копировать значение из input_columns[0] в output_column"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        df[output_column] = df[source_column]
    
    return df

def concatenate_columns(df, input_columns, output_column):
    """Конкатенировать несколько колонок"""
    if not input_columns:
        return df
    
    # Фильтруем существующие колонки
    existing_columns = [col for col in input_columns if col in df.columns]
    
    if existing_columns:
        # Конкатенируем с разделителем
        df[output_column] = df[existing_columns].astype(str).agg('_'.join, axis=1)
    
    return df

def fill_missing_with_zero(df, input_columns, output_column):
    """Заполнить пропуски нулями"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        df[output_column] = pd.to_numeric(df[source_column], errors='coerce').fillna(0)
    
    return df

def fill_missing_with_empty(df, input_columns, output_column):
    """Заполнить пропуски пустыми строками"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        df[output_column] = df[source_column].fillna('')
    
    return df

def convert_to_string(df, input_columns, output_column):
    """Конвертировать в строку"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        df[output_column] = df[source_column].astype(str)
    
    return df

def convert_to_int(df, input_columns, output_column):
    """Конвертировать в целое число"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        df[output_column] = pd.to_numeric(df[source_column], errors='coerce').fillna(0).astype(int)
    
    return df

def convert_to_float(df, input_columns, output_column):
    """Конвертировать в float, заменяя запятые на точки"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        # 1. Берем колонку
        # 2. Делаем всё строкой
        # 3. Меняем запятую на точку
        # 4. Превращаем во float64
        df[output_column] = (
            df[source_column]
            .astype(str)
            .str.replace(',', '.', regex=False)
            .astype('float64')
        )
    return df

def trim_string(df, input_columns, output_column):
    """Обрезать пробелы"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        df[output_column] = df[source_column].astype(str).str.strip()
    
    return df

def uppercase(df, input_columns, output_column):
    """В верхний регистр"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        df[output_column] = df[source_column].astype(str).str.upper()
    
    return df

def lowercase(df, input_columns, output_column):
    """В нижний регистр"""
    if not input_columns:
        return df
    
    source_column = input_columns[0]
    if source_column in df.columns:
        df[output_column] = df[source_column].astype(str).str.lower()
    
    return df

def calculate_sum(df, input_columns, output_column):
    """Суммировать несколько колонок"""
    if not input_columns:
        return df
    
    # Фильтруем существующие колонки
    existing_columns = [col for col in input_columns if col in df.columns]
    
    if existing_columns:
        # Конвертируем в числа и суммируем
        numeric_df = df[existing_columns].apply(pd.to_numeric, errors='coerce')
        df[output_column] = numeric_df.sum(axis=1, skipna=True).fillna(0)
    
    return df

def unrecognized_to_10003(df, input_columns, output_column):
    """
    Проверяет, является ли значение isnumeric().
    Если да - возвращает его как int, если нет - возвращает 10003.
    """
    if not input_columns or input_columns[0] not in df.columns:
        return df
    
    source_column = input_columns[0]
    
    def check_numeric(value):
        if pd.isna(value):
            return 10003
        # Проверяем строки
        if isinstance(value, str):
            # isnumeric() проверяет, что все символы в строке являются цифрами
            if value.isnumeric():
                return int(value)
            else:
                return 10003
        # Проверяем числа
        elif isinstance(value, (int, float)):
            return int(value) if not pd.isna(value) else 10003
        # Для остальных типов
        else:
            return 10003
    
    df[output_column] = df[source_column].apply(check_numeric)
    return df


def set_future_date(df, input_columns, output_column):
    """
    Устанавливает фиксированную дату 2099-01-01 в указанную колонку.
    Возвращает строку в формате YYYY-MM-DD.
    """
    # Явно задаем строку в нужном формате как object тип
    df[output_column] = '2100-01-01'
    # Принудительно устанавливаем тип object, чтобы pandas не конвертировал
    df[output_column] = df[output_column].astype('object')
    return df


def set_2000_date(df, input_columns, output_column):
    """
    Устанавливает фиксированную дату 2000-01-01 в указанную колонку.
    Возвращает строку в формате YYYY-MM-DD.
    """
    # Явно задаем строку в нужном формате как object тип
    df[output_column] = '2000-01-01'
    # Принудительно устанавливаем тип object, чтобы pandas не конвертировал
    df[output_column] = df[output_column].astype('object')
    return df

def coalesce_tin(df, input_columns, output_column):
    """
    Заменяет некорректные (не числовые) значения в основной колонке ИНН 
    на значения из резервной колонки.
    
    Ожидается, что input_columns = [основная_колонка, резервная_колонка]
    Например: ["tin_buyer", "apt_inn_ish_vis_ru"]
    
    Валидное значение — непустая строка, состоящая только из цифр.
    """
    if len(input_columns) != 2:
        raise ValueError("coalesce_tin требует ровно две входные колонки: [основная, резервная]")
    
    main_col, backup_col = input_columns

    # Функция проверки: состоит ли строка только из цифр (и не пустая)
    def is_numeric_str(x):
        if pd.isna(x) or x == '' or x is None:
            return False
        return str(x).isdigit()

    # Создаём копию основной колонки как объектный тип
    result = df[main_col].copy()

    # Маска: где основная колонка НЕ валидна
    invalid_mask = ~df[main_col].apply(is_numeric_str)

    # Где основная невалидна, но резервная — валидна → берём из резервной
    if backup_col in df.columns:
        backup_valid_mask = df[backup_col].apply(is_numeric_str)
        use_backup_mask = invalid_mask & backup_valid_mask
        result.loc[use_backup_mask] = df.loc[use_backup_mask, backup_col]
    else:
        # Если резервной колонки нет — оставляем как есть (или NaN)
        pass

    # Присваиваем результат
    df[output_column] = result.astype('object')
    return df

def get_id_pharmacy_for_not_russian_network(df, input_columns, output_column):
    """
    Для аптечных сетей с id_client 15 и 15385 присваивает id_pharmacy согласно
    внутреннему справочнику из БД. Для остальных id_client оставляет исходное значение.
    Если значение не найдено в БД, возвращает "НЕ ОПРЕДЕЛЕНО"
    """
    if len(input_columns) != 2:
        raise ValueError("Требуется две колонки: [сырое название, id_client]")

    raw_col, client_col = input_columns

    def get_new_value(row):
        if row[client_col] in ('15', '15385'):
            result = raw_pharmacy_crud.get_id_pharmacy(row[raw_col], row[client_col])
            return result if result is not None else "НЕ ОПРЕДЕЛЕНО"
        return row[output_column]  # оставляем исходное значение из колонки id_pharmacy

    df[output_column] = df.apply(get_new_value, axis=1)
    return df


# ============ РЕЕСТР ТРАНСФОРМЕРОВ ============

TRANSFORMERS = {
    'set_to_minus_one': set_to_minus_one,
    'set_to_zero': set_to_zero,
    'set_to_two': set_to_two,
    'set_to_three': set_to_three,
    'set_to_four': set_to_four,
    'set_to_five': set_to_five,
    'fill_missing_with_zero': fill_missing_with_zero,
    'fill_missing_with_empty': fill_missing_with_empty,
    'convert_to_string': convert_to_string,
    'convert_to_int': convert_to_int,
    'convert_to_float': convert_to_float,
    'trim_string': trim_string,
    'uppercase': uppercase,
    'lowercase': lowercase,
    'unrecognized_to_10003': unrecognized_to_10003,
    'set_future_date': set_future_date,
    'set_2000_date': set_2000_date,
    'coalesce_tin': coalesce_tin,
    'get_id_pharmacy_for_not_russian_network': get_id_pharmacy_for_not_russian_network,
}

def get_transformer(name):
    """Получить функцию трансформера по имени"""
    if name not in TRANSFORMERS:
        raise ValueError(f"Трансформер '{name}' не найден")
    return TRANSFORMERS[name]
