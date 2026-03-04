"""
Универсальные функции валидации
"""
import pandas as pd

# ============ УНИВЕРСАЛЬНЫЕ ВАЛИДАТОРЫ ============

def validate_greater_than_billion(df, column):
    """Проверка, что значение > 1000000000 (для id_pharmacy)"""
    return df[column].astype(int) > 1000000000

def validate_in_unrecognized(df, column):
    """Проверка, что значение в списке исключенных (для id_sku)"""
    excluded = ['~', 'ИСКЛЮЧИТЬ', '', 'НЕ ОПРЕДЕЛЕНО']
    return df[column].isin(excluded)

def validate_equals(df, column):
    """Проверка на равенство 'НЕ ОПРЕДЕЛЕНО' (для tin_pharmacy)"""
    return df[column] == 'НЕ ОПРЕДЕЛЕНО'

def validate_negative(df, column):
    """Проверка на отрицательные значения (для quantity)"""
    return df[column] < 0

def validate_missing(df, column):
    """Проверка на пропущенные значения"""
    return df[column].isna() | (df[column].astype(str).str.strip() == '')

def validate_not_numeric(df, column):
    """Возвращает True для значений, которые НЕ являются числами"""
    return pd.to_numeric(df[column], errors='coerce').isna()

def validate_is_2000000(df, column):
    """Возвращает True для значений, равных 2000000 (два миллиона)"""
    return df[column] == 2000000
# ============ РЕЕСТР ВАЛИДАТОРОВ ============

VALIDATORS = {
    'validate_greater_than_billion': validate_greater_than_billion,
    'validate_in_unrecognized': validate_in_unrecognized,
    'validate_equals': validate_equals,
    'validate_negative': validate_negative,
    'validate_missing': validate_missing,
    'validate_not_numeric': validate_not_numeric,
    'validate_is_2000000': validate_is_2000000
}

def get_validator(name):
    """Получить функцию валидатора по имени"""
    if name not in VALIDATORS:
        raise ValueError(f"Валидатор '{name}' не найден")
    return VALIDATORS[name]
