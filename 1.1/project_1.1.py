import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime
import time
import chardet

# Параметры подключения
DB_CONFIG = {
    'dbname': 'bank_db',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}


# Создание таблиц для хранения данных и логов
def create_tables():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS DS;
            CREATE SCHEMA IF NOT EXISTS LOGS;

            CREATE TABLE IF NOT EXISTS LOGS.LOAD_LOGS (
                log_id SERIAL PRIMARY KEY,
                table_name VARCHAR(50),
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                row_count BIGINT
            );

            CREATE TABLE IF NOT EXISTS DS.FT_BALANCE_F (
                on_date DATE NOT NULL,
                account_rk BIGINT NOT NULL,
                currency_rk BIGINT,
                balance_out FLOAT
            );

            CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
                oper_date DATE NOT NULL,
                credit_account_rk BIGINT NOT NULL,
                debet_account_rk BIGINT NOT NULL,
                credit_amount FLOAT,
                debet_amount FLOAT
            );

            CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
                data_actual_date DATE NOT NULL,
                data_actual_end_date DATE NOT NULL,
                account_rk BIGINT NOT NULL,
                account_number VARCHAR(20),
                char_type VARCHAR(1),
                currency_rk BIGINT,
                currency_code VARCHAR(3)
            );
        """)
        conn.commit()
        print("Таблицы успешно созданы.")
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")
    finally:
        cur.close()
        conn.close()


# Функция для логирования
def log_load(table_name, start_time, end_time, row_count):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO LOGS.LOAD_LOGS (table_name, start_time, end_time, row_count)
            VALUES (%s, %s, %s, %s)
        """, (table_name, start_time, end_time, row_count))
        conn.commit()
        print(f"Загрузка данных для {table_name} завершена.")
    except Exception as e:
        print(f"Ошибка при логировании: {e}")
    finally:
        cur.close()
        conn.close()


# Функция для загрузки данных из CSV


def load_csv_to_postgres(file_path, table_name):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        # Определение кодировки
        with open(file_path, 'rb') as f:
            encoding = chardet.detect(f.read())['encoding']

        # Чтение файла с корректной кодировкой
        df = pd.read_csv(file_path, sep=';', engine='python', encoding=encoding)

        start_time = datetime.now()

        # Подготовка запроса на вставку
        cols = df.columns.tolist()
        insert_query = sql.SQL(f"""
            INSERT INTO DS.{table_name} ({', '.join(cols)})
            VALUES ({', '.join(['%s'] * len(cols))})
        """)

        # Вставка данных
        cur.executemany(insert_query, df.to_records(index=False).tolist())

        conn.commit()
        row_count = len(df)
        end_time = datetime.now()

        # Логирование
        log_load(table_name, start_time, end_time, row_count)
        print(f"{row_count} записей успешно загружено в таблицу {table_name}.")

    except Exception as e:
        print(f"Ошибка при загрузке данных в {table_name}: {e}")
    finally:
        cur.close()
        conn.close()




# Основная функция ETL
def run_etl():
    create_tables()
    time.sleep(2)  # Для разделения логов по времени
    load_csv_to_postgres('E:/Treespare/Рабочий стол/Новая папка (6)/Проект 1/файлы/md_ledger_account_s.csv', 'MD_LEDGER_ACCOUNT_S')
    time.sleep(2)
    load_csv_to_postgres('E:/Treespare/Рабочий стол/Новая папка (6)/Проект 1/файлы/ft_balance_f.csv', 'FT_BALANCE_F')
    time.sleep(2)
    load_csv_to_postgres('E:/Treespare/Рабочий стол/Новая папка (6)/Проект 1/файлы/ft_posting_f.csv', 'FT_POSTING_F')


if __name__ == "__main__":
    run_etl()
