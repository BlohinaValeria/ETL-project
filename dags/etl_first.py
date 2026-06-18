from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
from datetime import datetime as dt

from clickhouse_driver import Client

default_args = {
    'owner': 'analytics_team',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 1, 1),
}

schedule_interval = '0 8 * * *'

# Директория для временных файлов
TEMP_DIR = '/tmp/airflow_data'
os.makedirs(TEMP_DIR, exist_ok=True)


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['tezis', 'repair_analysis'])
def tec_breakdown_etl():
    @task
    def extract_from_excel():
        excel_path = '/opt/airflow/data/select_d_unique_number_sv_filial_sv_station_sv_oe_e_full_name_d.xlsx'

        print(f"📖 Reading file: {excel_path}")
        df = pd.read_excel(excel_path, dtype=str, engine='openpyxl')
        print(f"✅ Extracted {len(df)} rows, {len(df.columns)} columns")

        # Сохраняем в Parquet
        output_path = os.path.join(TEMP_DIR, 'extracted_data.parquet')
        df.to_parquet(output_path, index=False)
        print(f"💾 Saved to: {output_path}")

        return output_path

    @task
    def transform_data(input_path):
        print(f"📂 Reading from: {input_path}")
        df = pd.read_parquet(input_path)
        print(f"📊 Initial shape: {df.shape}")

        column_mapping = {
            'unique_number': 'defect_id',
            'filial': 'branch',
            'station': 'station',
            'oe': 'department',
            'full_name': 'equipment_name',
            'description': 'defect_description',
            'created_date': 'created_date_raw',
            'alias': 'status',
            'fio': 'responsible_person',
            'priority': 'priority_raw',
            'Плановый_срок_устранения_дефекта': 'planned_deadline_raw',
            'Фактический_срок_устранения_дефек': 'actual_fix_date_raw'
        }

        # Переименовываем только существующие колонки
        existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_mapping)

        # Выбираем нужные колонки
        cols_to_keep = ['defect_id', 'branch', 'station', 'department', 'equipment_name',
                        'defect_description', 'status', 'responsible_person', 'priority_raw',
                        'created_date_raw', 'planned_deadline_raw', 'actual_fix_date_raw']
        df = df[[col for col in cols_to_keep if col in df.columns]]

        df['created_date'] = pd.to_datetime(df['created_date_raw'], errors='coerce', dayfirst=True)
        df['planned_deadline'] = pd.to_datetime(df['planned_deadline_raw'], errors='coerce', dayfirst=True)
        df['actual_fix_date'] = pd.to_datetime(df['actual_fix_date_raw'], errors='coerce', dayfirst=True)

        df['priority'] = pd.to_numeric(df['priority_raw'], errors='coerce')
        df['priority'] = df['priority'].fillna(0).astype(int)

        # Расчет метрик
        df['fix_time_days'] = (df['actual_fix_date'] - df['created_date']).dt.days
        df['overdue_days'] = (df['actual_fix_date'] - df['planned_deadline']).dt.days
        df['is_overdue'] = np.where(df['overdue_days'] > 0, 1, 0)
        df['is_plan_met'] = np.where(df['actual_fix_date'] <= df['planned_deadline'], 1, 0)

        # Категоризация дефектов
        df['defect_description'] = df['defect_description'].fillna('')
        df['defect_category'] = 'other'
        df.loc[df['defect_description'].str.contains('te|svisch', case=False, na=False), 'defect_category'] = 'leak'
        df.loc[df['defect_description'].str.contains('salnik', case=False, na=False), 'defect_category'] = 'seal'
        df.loc[df['defect_description'].str.contains('zadvizhk|ventil|armatur', case=False,
                                                     na=False), 'defect_category'] = 'valve'
        df.loc[df['defect_description'].str.contains('elektric|kabel|osvescheni', case=False,
                                                     na=False), 'defect_category'] = 'electrical'

        # Фильтрация
        df = df.dropna(subset=['created_date'])
        df_eliminated = df[df['status'] == 'ELIMINATED'].copy()

        # Очистка от выбросов
        df_eliminated['fix_time_days'] = df_eliminated['fix_time_days'].fillna(0)
        df_eliminated['overdue_days'] = df_eliminated['overdue_days'].fillna(0)
        df_eliminated.loc[df_eliminated['fix_time_days'] < 0, 'fix_time_days'] = 0
        df_eliminated.loc[df_eliminated['fix_time_days'] > 1000, 'fix_time_days'] = 0
        df_eliminated.loc[df_eliminated['overdue_days'] < -1000, 'overdue_days'] = 0

        df_eliminated['fix_time_days'] = df_eliminated['fix_time_days'].astype(int)
        df_eliminated['overdue_days'] = df_eliminated['overdue_days'].astype(int)
        df_eliminated['is_overdue'] = df_eliminated['is_overdue'].astype(int)
        df_eliminated['is_plan_met'] = df_eliminated['is_plan_met'].astype(int)

        string_cols = ['responsible_person', 'branch', 'station', 'department', 'equipment_name']
        for col in string_cols:
            df_eliminated[col] = df_eliminated[col].fillna('')

        df_eliminated = df_eliminated.drop(
            columns=['created_date_raw', 'planned_deadline_raw', 'actual_fix_date_raw', 'priority_raw'])

        print(f"✅ Transformation complete. {len(df_eliminated)} records to load")

        output_path = os.path.join(TEMP_DIR, 'transformed_data.parquet')
        df_eliminated.to_parquet(output_path, index=False)
        print(f"💾 Saved transformed data to: {output_path}")

        return output_path

    @task
    def load_data(input_path):
        """Загрузка данных в ClickHouse"""
        print(f"📂 Reading transformed data from: {input_path}")
        df = pd.read_parquet(input_path)
        print(f"📊 Loading {len(df)} records to ClickHouse")

        # Подключаемся к ClickHouse
        client = Client(host='clickhouse', port=9000, user='default', password='')
        print("✅ Connected to ClickHouse")

        # Создаем таблицу с Nullable типами для дат
        client.execute("""
            CREATE TABLE IF NOT EXISTS defect_analysis (
                defect_id String,
                branch String,
                station String,
                department String,
                equipment_name String,
                defect_description String,
                status String,
                responsible_person String,
                priority Int32,
                created_date Nullable(Date),
                planned_deadline Nullable(Date),
                actual_fix_date Nullable(Date),
                fix_time_days Int32,
                overdue_days Int32,
                is_overdue Int32,
                is_plan_met Int32,
                defect_category String
            ) ENGINE = MergeTree()
            ORDER BY (branch, created_date)
        """)
        print("✅ Table created/verified")

        columns_order = [
            'defect_id', 'branch', 'station', 'department', 'equipment_name',
            'defect_description', 'status', 'responsible_person', 'priority',
            'created_date', 'planned_deadline', 'actual_fix_date', 'fix_time_days',
            'overdue_days', 'is_overdue', 'is_plan_met', 'defect_category'
        ]


        for date_col in ['created_date', 'planned_deadline', 'actual_fix_date']:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                # Преобразуем в date объекты или None
                df[date_col] = df[date_col].apply(lambda x: x.date() if pd.notna(x) else None)

        # Заполняем числовые NaN нулями
        numeric_cols = ['priority', 'fix_time_days', 'overdue_days', 'is_overdue', 'is_plan_met']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(int)

        # Заполняем строковые NaN пустыми строками
        string_cols = ['defect_id', 'branch', 'station', 'department', 'equipment_name',
                       'defect_description', 'status', 'responsible_person', 'defect_category']
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].fillna('').astype(str)

        # Создаем список кортежей для вставки
        data_to_insert = []
        problematic_records = 0

        for _, row in df.iterrows():
            row_data = []
            valid = True
            for col in columns_order:
                value = row[col]

                # Проверка валидности дат
                if col in ['created_date', 'planned_deadline', 'actual_fix_date']:
                    if value is not None:
                        try:
                            # Проверяем, что это валидный date объект
                            if hasattr(value, 'year'):
                                _ = value.year
                            else:
                                value = None
                        except:
                            value = None

                row_data.append(value)

            data_to_insert.append(tuple(row_data))

        print(f"✅ Prepared {len(data_to_insert)} records for insertion")

        if len(data_to_insert) == 0:
            print("⚠️ No valid data to insert")
            return "No data loaded"

        # Вставляем данные пакетами
        batch_size = 500
        total_inserted = 0

        for i in range(0, len(data_to_insert), batch_size):
            batch = data_to_insert[i:i + batch_size]
            try:
                insert_query = f"""
                    INSERT INTO defect_analysis ({', '.join(columns_order)}) 
                    VALUES
                """
                client.execute(insert_query, batch)
                total_inserted += len(batch)
                print(f"✅ Inserted batch {i // batch_size + 1}: {len(batch)} records")
            except Exception as e:
                print(f"❌ Error inserting batch {i // batch_size + 1}: {e}")
                # Пробуем вставить по одной записи из проблемного батча
                print("Attempting to insert records one by one...")
                for record in batch:
                    try:
                        client.execute(insert_query, [record])
                        total_inserted += 1
                    except Exception as record_error:
                        problematic_records += 1
                        print(
                            f"Failed to insert record: {record[:5] if len(record) > 5 else record}... Error: {record_error}")
                print(f"Successfully inserted {total_inserted} records after fallback")

        print(f"✅ Successfully inserted {total_inserted} records (skipped {problematic_records} problematic records)")

        # Проверяем количество записей
        count = client.execute("SELECT COUNT(*) FROM defect_analysis")[0][0]
        print(f"✅ Total records in table: {count}")

        # Очищаем временные файлы
        try:
            os.remove(input_path)
            extracted_file_path = os.path.join(TEMP_DIR, 'extracted_data.parquet')
            if os.path.exists(extracted_file_path):
                os.remove(extracted_file_path)
            print("🧹 Temporary files cleaned up")
        except Exception as e:
            print(f"⚠️ Could not clean temp files: {e}")

        return f"Loaded {total_inserted} records, skipped {problematic_records}"

    # Определяем DAG
    extracted_file = extract_from_excel()
    transformed_file = transform_data(extracted_file)
    load_data(transformed_file)


# Создаем экземпляр DAG
tec_breakdown_etl = tec_breakdown_etl()