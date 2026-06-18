import os

SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY', 'your-secret-key-change-this-in-production')

# Используем SQLite для метаданных (проще для начала)
SQLALCHEMY_DATABASE_URI = 'sqlite:////app/superset_home/superset.db'

# Отключаем загрузку примеров
SUPERSET_LOAD_EXAMPLES = False

# Настройки для ClickHouse
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Таймзона
DEFAULT_TIMEZONE = 'Europe/Moscow'

# Лимит строк
ROW_LIMIT = 10000

# Кэширование
CACHE_CONFIG = {
    'CACHE_TYPE': 'SimpleCache',
    'CACHE_DEFAULT_TIMEOUT': 300,
}

# Экспорт данных
CSV_EXPORT = {
    "encoding": "utf-8",
    "sep": ",",
    "null": "",
    "quoting": "minimal"
}
EXCEL_EXPORT = True