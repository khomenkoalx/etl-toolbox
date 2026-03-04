# ETL Toolbox для данных от внешнего аналитического подрядчика


[![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-D71F00?style=for-the-badge&logo=sqlalchemy&logoColor=white)](https://www.sqlalchemy.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)



---
## ОПИСАНИЕ СИСТЕМЫ

Данный инструмент автоматически обрабатывает сырые CSV-файлы с данными о закупках, продажах и остатках товаров компании по аптекам РФ. Система выполняет:

| Функция                 | Описание                                                    |
|-------------------------|-------------------------------------------------------------|
| Чанковая обработка      | Поточная загрузка больших CSV-файлов без переполнения памяти |
| Конфигурируемая трансформация | Правила маппинга колонок задаются в JSON (без правок в коде) |
| Валидация на лету       | Отсеивание невалидных строк в отдельные отчёты               |
| Обогащение              | Нормализация адресов через Dadata API и lookup клиентов через PostgreSQL |


## ТЕХНОЛОГИЧЕСКИЙ СТЕК

| Компонент           | Версия    | Назначение                                       |
|---------------------|-----------|--------------------------------------------------|
| Python              | 3.12+     | Язык реализации                                  |
| Pandas              | 2.3.3     | Чтение и трансформация табличных данных          |
| SQLAlchemy          | 2.0.45    | ORM-доступ к справочникам                        |
| Pydantic-settings   | 2.11.0    | Валидация настроек из .env                       |
| Dadata SDK          | 25.10.0   | Обогащение гео-данных                            |
| Click               | 8.0.0     | CLI-интерфейс                                    |
| tqdm                | 4.65.0    | Прогресс-бары обработки                          |


## СТРУКТУРА ПРОЕКТА

```plain
etl-toolbox/
├── core/                  # Инфраструктурные модули
│   ├── config.py         # Загрузка .env через pydantic
│   ├── db.py             # SessionLocal + engine
│   ├── models.py         # SQLAlchemy ORM модели
│   ├── crud.py           # Mapper-логика lookup id_pharmacy, id_client
│   ├── dadata_client.py  # Обёртка над Dadata API
│   └── smtp_client.py    # Уведомления по email
├── etl/                   # Основной движок ETL
│   ├── configs/          # JSON-конфиги для каждого типа данных
│   ├── processor.py      # DataProcessor — центральный класс
│   ├── transformers.py   # Простые трансформеры
│   ├── special_transformers.py # Зависимые от БД/API трансформеры
│   └── validators.py     # Валидаторы
├── data/                  # Каталог файловых операций
│   ├── input/            # Входные CSV от подрядчика
│   ├── validated/        # Очищенные данные
│   ├── errors/           # Ошибочные записи
│   └── processed/        # Архив обработанных исходников
├── validate.py           # Стандартная обработка всех файлов
├── enrich_addresses.py   # Скрипт обогащения адресов через Dadata
├── requirements.txt      # Зависимости
├── Dockerfile            # Контейнерное окружение
└── .env.example          # Шаблон переменных окружения
```

## БЫСТРЫЙ СТАРТ

1. КЛОНИРОВАНИЕ И УСТАНОВКА ЗАВИСИМОСТЕЙ
```bash
git clone https://github.com/khomenkoalx/etl_toolbox
cd etl_toolbox

python -m venv venv
source venv/bin/activate          # Linux/macOS
venv\Scripts\activate             # Windows

pip install -r requirements.txt
```

2. НАСТРОЙКА ОКРУЖЕНИЯ

Создайте файл .env на основе шаблона .env.example:

```plain
=== База данных ===
DB_CONNECTION_STRING=postgresql+psycopg2://user:password@host:5432/dbname

=== Dadata API ===
DADATA_TOKEN=your_dadata_token_here
DADATA_SECRET=your_dadata_secret_here

=== Email уведомления ===
EMAIL_SENDER=noreply@company.ru
EMAIL_PASSWORD=your_email_password
EMAIL_RECEIVER=analytics@company.ru
SMTP_HOST=smtp.company.com
SMTP_PORT=587
```


3. ПОДГОТОВКА ДАННЫХ

Разместите входные CSV в директорию data/input/:
```plain
data/input/
├── ЗАКУПКИ_январь.csv
├── ПРОДАЖИ_февраль.csv
├── ОСТАТКИ_март.csv
├── ОСТАТКИ ДБ_april.csv
├── ПРОДАЖИ ДБ_may.csv
├── ВОЗВРАТЫ ДБ_june.csv
└── ТРАНЗИТ ДБ_july.csv
```

Файлы должны начинаться с префикса типа данных:
ЗАКУПКИ, ПРОДАЖИ, ОСТАТКИ, ОСТАТКИ ДБ, ПРОДАЖИ ДБ, ВОЗВРАТЫ ДБ, ТРАНЗИТ ДБ

---
## РЕЖИМЫ РАБОТЫ


### Через validate.py (обработка всех файлов в data/input/):
    python validate.py


## Обогащение адресов через Dadata (пополнение Базы FIAS-кодов):
    python enrich_addresses.py

---
## КОНФИГУРАЦИЯ ETL

Правила обработки задаются в файле etl/configs/etl_config.json.
Каждый тип данных имеет свой блок с правилами маппинга, трансформаций и валидации. Последовательность обработки данных следующая:  
1. Переименование исходных колонок по правилам из словаря `column_mapping`.  
2. Применение трансформаций из словаря `transformation_rules`.  
3. Выполнение валидаций из словаря `validation_rules` с маршрутизацией невалидных строк в папку `errors`.


ПРИМЕР КОНФИГА: «ЗАКУПКИ»
```json
{
  "ЗАКУПКИ": {
    "output_file_name": "ЗАКУПКИ",
    "column_mapping": {
      "period_ish_vis_ru": "operation_date",
      "net_id_vis_vis_ru": "id_client",
      "aptid_vis_ru": "id_pharmacy",
      "tms_id_vis_ru": "id_sku",
      "sum_amount_e": "quantity"
    },
    "transformation_rules": [
      {
        "transformer": "set_to_minus_one",
        "input_columns": ["id_branch"],
        "output_column": "id_branch"
      },
      {
        "transformer": "coalesce_tin",
        "input_columns": ["tin_pharmacy", "apt_inn_ish_vis_ru"],
        "output_column": "tin_pharmacy"
      }
    ],
    "validation_rules": [
      {
        "validator": "validate_in_unrecognized",
        "column": "id_sku"
      },
      {
        "validator": "validate_not_numeric",
        "column": "id_sku"
      }
    ]
  }
}
```

### ТРАНСФОРМЕРЫ
Трансформер в контексте данного проекта это функция, принимающая на вход Pandas-датафрейм, список названий исходных колонок и строковое название целевой колонки.  
Примеры существующих трансформеров:  

| Трансформер                     | Описание                                        |
|---------------------------------|-------------------------------------------------|
| set_to_minus_one                | Установить -1 для всех значений             |
| set_to_zero                     | Установить 0 для всех значений             |
| set_to_two, set_to_three, ...   | Установка фиксированных ID типов операций       |
| unrecognized_to_10003           | Замена нераспознанных значений на ID 10003        |
| coalesce_tin                    | Объединение ИНН из двух источников (аналог побитового ИЛИ)              |
| get_id_pharmacy_for_not_russian | Lookup аптек для иностранных сетей              |
| set_future_date, set_2000_date  | Установка выбранной даты для всех значений           |


### ВАЛИДАТОРЫ
В контексте данного проекта валидатор это функция, принимающая на вход Pandas-датафрейм и строкое название колонки и возвращающая логическую маску невалидных значений.

Примеры существующих валидаторов:  
| Валидатор                      | Описание                                     |
|--------------------------------|----------------------------------------------|
| validate_in_unrecognized       | Проверка на специальные значения (`~`, `НЕ ОПРЕДЕЛЕНО`) |
| validate_not_numeric           | Проверка на нечисловое значение |
| validate_greater_than_billion  | Контроль диапазона (> 1 000 000 000)         |


--- 
## РАЗВЕРТЫВАНИЕ ПРОЕКТА ЧЕРЕЗ DOCKER


СБОРКА ОБРАЗА И ЗАГРУЗКА В DOCKERHUB

```bash
docker build -t $(YOUR-REGISTRY-NAME)/etl-toolbox:latest .
docker login
docker push $(YOUR-REGISTRY-NAME)/etl-toolbox:latest
```

ЗАПУСК КОНТЕЙНЕРА

```bash
docker run -it \
  --env-file .env \
  -v $(pwd)//app/data \
  $(YOUR-REGISTRY-NAME)/etl-toolbox:latest \
  python validate.py
```

---
## МАРШРУТИЗАЦИЯ ДАННЫХ


| Исходный файл   | Выходной путь                      | Описание                       |
|-----------------|-----------------------------------|--------------------------------|
| ОСТАТКИ         | data/validated/ОСТАТКИ.csv        | Остатки в аптеках сетей        |
| ОСТАТКИ ДБ      | data/validated/ОСТАТКИ ДБ.csv     | Остатки на складах дистрибьюторов |
| ТРАНЗИТ ДБ      | data/validated/ТРАНЗИТ ДБ.csv     | Товар в пути до склада дистрибьютора      |
| ПРОДАЖИ         | data/validated/ПРОДАЖИ.csv        | Третичные продажи (от аптечных сетей физическим лицам)    |
| ПРОДАЖИ ДБ      | data/validated/ПРОДАЖИ ДБ.csv     | Вторичные продажи (от дистрибьюторов аптечным сетям)      |
| ВОЗВРАТЫ ДБ     | data/validated/ВОЗВРАТЫ ДБ.csv    | Возвраты от сетей на склады дистрибьюторов|
| ЗАКУПКИ         | data/validated/ЗАКУПКИ.csv        | Закупки аптеками у дистрибьюторов/компании               |

---

## МЕТРИКИ ВЫВОДА


После обработки система выводит статистику:
```bash
📊 Всего строк: 1,234,567
📦 Обработано чанков: 124
✅ Валидных: 1,200,000 (97.2%)
❌ Ошибок: 34,567 (2.8%)

⚠️ ТИПЫ ОШИБОК:
   • validate_in_unrecognized: 15,000 (43.4%)
   • validate_not_numeric: 10,000 (28.9%)
   • validate_greater_than_billion: 9,567 (27.7%)

⏱️ Время обработки: 185.43 сек
📈 Скорость: 6,658 строк/сек
```

Результаты сохраняются:
- ✅ Валидные данные → data/validated/<тип>_DATE_START_DATE_END.csv
- ❌ Ошибки → data/errors/<тип>_errors_DATE_START_DATE_END.csv


---
## ДОБАВЛЕНИЕ НОВОГО ТИПА ДАННЫХ

1. ПОДГОТОВЬТЕ ВХОДНОЙ ФАЙЛ  
   Убедитесь, что имя начинается с уникального префикса (например, 
   НОВЫЙ ДОГОВОР.csv)

2. ДОБАВЬТЕ БЛОК В `etl/configs/etl_config.json`:

```json
{
  "НОВЫЙ ДОГОВОР": {
    "output_file_name": "новый_договор",
    "column_mapping": { ... },
    "transformation_rules": [ ... ],
    "validation_rules": [ ... ]
  }
}
```

3. ЗАПУСТИТЕ ОБРАБОТКУ  
   Новый тип определится автоматически через `validate.py`


## 👤 Об авторе

<div align="left">

---

**Хоменко Александр**  
📍 *Санкт-Петербург, Россия*  
🐍 *Python Developer | Data Engineering | ETL Architect*

| Канал связи | Ссылка |
|---------|--------|
| GitHub | [github.com/khomenkoalx](https://github.com/khomenkoalx) |
| Telegram | [@drkhomenko](https://t.me/drkhomenko) |
| Email | [a.khomenko42@gmail.com](mailto:a.khomenko42@gmail.com) |
</div>