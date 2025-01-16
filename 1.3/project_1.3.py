import psycopg2  # Для работы с PostgreSQL
import pandas as pd  # Для работы с CSV-файлами
from datetime import datetime, timedelta  # Для работы с датами
import chardet  # Для автоматического определения кодировки


# Параметры подключения к базе данных
DB_CONFIG = {
    'dbname': 'bank_db',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}

# Функция для загрузки данных из CSV в PostgreSQL

def load_exchange_rate_data(file_path):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        # Определение кодировки файла
        with open(file_path, 'rb') as f:
            detected_encoding = chardet.detect(f.read())['encoding']

        print(f"Кодировка файла {file_path}: {detected_encoding}")

        # Чтение данных из файла с определённой кодировкой
        df = pd.read_csv(file_path, encoding=detected_encoding)

        # Проверка структуры столбцов
        expected_columns = ['data_actual_date', 'data_actual_end_date', 'currency_rk', 'reduced_cource', 'code_iso_num']
        if not all(col in df.columns for col in expected_columns):
            raise ValueError(f"Файл не содержит все необходимые столбцы: {expected_columns}")

        # Загрузка данных в PostgreSQL
        rows = df.to_records(index=False).tolist()
        cur.executemany("""
            INSERT INTO DS.MD_EXCHANGE_RATE_D (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)
            VALUES (%s, %s, %s, %s, %s)
        """, rows)

        conn.commit()
        print(f"Успешно загружено {len(rows)} строк в таблицу DS.MD_EXCHANGE_RATE_D.")
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
    finally:
        cur.close()
        conn.close()



file_path = r'E:\Treespare\Рабочий стол\Новая папка (6)\Проект 1\файлы\md_exchange_rate_d.csv'
load_exchange_rate_data(file_path)


# Функция для выполнения SQL-запросов
def execute_query(query, params=None):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        conn.commit()
        print("Запрос выполнен успешно.")
    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
    finally:
        cur.close()
        conn.close()

# Создание витрин
def create_vitrines():
    query = """
    CREATE SCHEMA IF NOT EXISTS DM;

    CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_TURNOVER_F (
        on_date DATE NOT NULL,
        account_rk BIGINT NOT NULL,
        credit_amount FLOAT,
        credit_amount_rub FLOAT,
        debet_amount FLOAT,
        debet_amount_rub FLOAT
    );

    CREATE TABLE IF NOT EXISTS DM.DM_ACCOUNT_BALANCE_F (
        on_date DATE NOT NULL,
        account_rk BIGINT NOT NULL,
        balance_out FLOAT,
        balance_out_rub FLOAT
    );
    """
    execute_query(query)

# Создание необходимых таблиц
def create_required_tables():
    query = """
    CREATE SCHEMA IF NOT EXISTS DS;

    CREATE TABLE IF NOT EXISTS DS.MD_EXCHANGE_RATE_D (
        data_actual_date DATE NOT NULL,
        data_actual_end_date DATE,
        currency_rk BIGINT NOT NULL,
        reduced_cource FLOAT,
        code_iso_num VARCHAR(3)
    );

    CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
        data_actual_date DATE NOT NULL,
        data_actual_end_date DATE,
        account_rk BIGINT NOT NULL,
        char_type CHAR(1),
        currency_rk BIGINT
    );
    """
    execute_query(query)

def fill_initial_balance():
    print(f"\n--- Начало заполнения витрины остатков на 31.12.2017 ---")

    query = """
        DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = '2017-12-31';

        INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
        SELECT 
            '2017-12-31',
            f.account_rk,
            f.balance_out,
            f.balance_out * COALESCE((
                SELECT reduced_cource 
                FROM DS.MD_EXCHANGE_RATE_D 
                WHERE data_actual_date = '2017-12-31' AND currency_rk = f.currency_rk
            ), 1) AS balance_out_rub
        FROM DS.FT_BALANCE_F f;
    """

    try:
        execute_query(query)
        print(f"\n✅ Витрина остатков за 31.12.2017 успешно заполнена!\n")
    except Exception as e:
        print(f"\n❌ Ошибка при заполнении витрины за 31.12.2017: {e}")

def fill_account_turnover_f(i_OnDate):
    print(f"Начало расчета витрины оборотов на дату {i_OnDate}")
    query = """
        DELETE FROM DM.DM_ACCOUNT_TURNOVER_F WHERE on_date = %s;

        INSERT INTO DM.DM_ACCOUNT_TURNOVER_F (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
        SELECT 
            %s AS on_date,
            credit_account_rk AS account_rk,
            SUM(credit_amount) AS credit_amount,
            SUM(credit_amount * COALESCE(
                (SELECT reduced_cource 
                 FROM DS.MD_EXCHANGE_RATE_D 
                 WHERE data_actual_date = %s 
                 AND currency_rk = p.currency_rk),
                1)) AS credit_amount_rub,
            SUM(debet_amount) AS debet_amount,
            SUM(debet_amount * COALESCE(
                (SELECT reduced_cource 
                 FROM DS.MD_EXCHANGE_RATE_D 
                 WHERE data_actual_date = %s 
                 AND currency_rk = p.currency_rk),
                1)) AS debet_amount_rub
        FROM DS.FT_POSTING_F p
        WHERE p.oper_date = %s
        GROUP BY credit_account_rk, p.currency_rk;
    """
    execute_query(query, (i_OnDate, i_OnDate, i_OnDate, i_OnDate, i_OnDate))


# Функция расчета витрины остатков
def fill_account_balance_f(i_OnDate):
    print(f"\n--- Начало расчета витрины остатков на дату: {i_OnDate} ---")

    query = """
        DELETE FROM DM.DM_ACCOUNT_BALANCE_F WHERE on_date = %s;

        INSERT INTO DM.DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
        SELECT 
            %s AS on_date,
            a.account_rk,
            CASE 
                WHEN a.char_type = 'А' THEN 
                    COALESCE((
                        SELECT balance_out 
                        FROM DM.DM_ACCOUNT_BALANCE_F 
                        WHERE on_date = %s - INTERVAL '1 day' AND account_rk = a.account_rk
                    ), 0) 
                    + COALESCE((
                        SELECT SUM(debet_amount) 
                        FROM DM.DM_ACCOUNT_TURNOVER_F 
                        WHERE on_date = %s AND account_rk = a.account_rk
                    ), 0)
                    - COALESCE((
                        SELECT SUM(credit_amount) 
                        FROM DM.DM_ACCOUNT_TURNOVER_F 
                        WHERE on_date = %s AND account_rk = a.account_rk
                    ), 0)
                WHEN a.char_type = 'П' THEN 
                    COALESCE((
                        SELECT balance_out 
                        FROM DM.DM_ACCOUNT_BALANCE_F 
                        WHERE on_date = %s - INTERVAL '1 day' AND account_rk = a.account_rk
                    ), 0) 
                    - COALESCE((
                        SELECT SUM(debet_amount) 
                        FROM DM.DM_ACCOUNT_TURNOVER_F 
                        WHERE on_date = %s AND account_rk = a.account_rk
                    ), 0)
                    + COALESCE((
                        SELECT SUM(credit_amount) 
                        FROM DM.DM_ACCOUNT_TURNOVER_F 
                        WHERE on_date = %s AND account_rk = a.account_rk
                    ), 0)
            END AS balance_out,
            CASE 
                WHEN a.char_type = 'А' THEN 
                    COALESCE((
                        SELECT balance_out_rub 
                        FROM DM.DM_ACCOUNT_BALANCE_F 
                        WHERE on_date = %s - INTERVAL '1 day' AND account_rk = a.account_rk
                    ), 0) 
                    + COALESCE((
                        SELECT SUM(debet_amount_rub) 
                        FROM DM.DM_ACCOUNT_TURNOVER_F 
                        WHERE on_date = %s AND account_rk = a.account_rk
                    ), 0)
                    - COALESCE((
                        SELECT SUM(credit_amount_rub) 
                        FROM DM.DM_ACCOUNT_TURNOVER_F 
                        WHERE on_date = %s AND account_rk = a.account_rk
                    ), 0)
                WHEN a.char_type = 'П' THEN 
                    COALESCE((
                        SELECT balance_out_rub 
                        FROM DM.DM_ACCOUNT_BALANCE_F 
                        WHERE on_date = %s - INTERVAL '1 day' AND account_rk = a.account_rk
                    ), 0) 
                    - COALESCE((
                        SELECT SUM(debet_amount_rub) 
                        FROM DM.DM_ACCOUNT_TURNOVER_F 
                        WHERE on_date = %s AND account_rk = a.account_rk
                    ), 0)
                    + COALESCE((
                        SELECT SUM(credit_amount_rub) 
                        FROM DM.DM_ACCOUNT_TURNOVER_F 
                        WHERE on_date = %s AND account_rk = a.account_rk
                    ), 0)
            END AS balance_out_rub
        FROM DS.MD_ACCOUNT_D a
        WHERE %s BETWEEN a.data_actual_date AND COALESCE(a.data_actual_end_date, %s);
    """

    # ✅ Исправленный список параметров (16 штук для 16 %s)
    params = (
        i_OnDate, i_OnDate, i_OnDate, i_OnDate, i_OnDate,
        i_OnDate, i_OnDate, i_OnDate, i_OnDate, i_OnDate,
        i_OnDate, i_OnDate, i_OnDate, i_OnDate, i_OnDate, i_OnDate
    )

    # ✅ Выполнение запроса с защитой от ошибок
    try:
        execute_query(query, params)
        print(f"\n✅ Успешное завершение расчета витрины остатков для даты: {i_OnDate}\n")
    except Exception as e:
        print(f"\n❌ Ошибка выполнения запроса: {e}")

def fill_f101_round_f(i_OnDate):
    print(f"\n--- Начало расчета 101 формы на дату: {i_OnDate} ---")

    query = """
        DELETE FROM DM.DM_F101_ROUND_F WHERE from_date = CAST(%s AS DATE);

        INSERT INTO DM.DM_F101_ROUND_F (
            from_date, to_date, chapter, ledger_account, characteristic, 
            balance_in_rub, balance_in_val, balance_in_total, 
            turn_deb_rub, turn_deb_val, turn_deb_total, 
            turn_cre_rub, turn_cre_val, turn_cre_total, 
            balance_out_rub, balance_out_val, balance_out_total
        )
        SELECT 
            DATE_TRUNC('month', CAST(%s AS DATE)) AS from_date,  
            CAST(%s AS DATE) - INTERVAL '1 day' AS to_date,      
            l.chapter,
            LEFT(a.account_number, 5) AS ledger_account,
            a.char_type AS characteristic,

            -- Приведение типов исправлено и согласовано с агрегатными функциями
            SUM(CASE WHEN CAST(a.currency_code AS INTEGER) IN (810, 643) THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
            SUM(CASE WHEN CAST(a.currency_code AS INTEGER) NOT IN (810, 643) THEN b.balance_out_rub ELSE 0 END) AS balance_in_val,
            SUM(b.balance_out_rub) AS balance_in_total,

            SUM(CASE WHEN CAST(a.currency_code AS INTEGER) IN (810, 643) THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
            SUM(CASE WHEN CAST(a.currency_code AS INTEGER) NOT IN (810, 643) THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
            SUM(t.debet_amount_rub) AS turn_deb_total,

            SUM(CASE WHEN CAST(a.currency_code AS INTEGER) IN (810, 643) THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
            SUM(CASE WHEN CAST(a.currency_code AS INTEGER) NOT IN (810, 643) THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
            SUM(t.credit_amount_rub) AS turn_cre_total,

            SUM(CASE WHEN CAST(a.currency_code AS INTEGER) IN (810, 643) THEN b_end.balance_out_rub ELSE 0 END) AS balance_out_rub,
            SUM(CASE WHEN CAST(a.currency_code AS INTEGER) NOT IN (810, 643) THEN b_end.balance_out_rub ELSE 0 END) AS balance_out_val,
            SUM(b_end.balance_out_rub) AS balance_out_total

        FROM DS.MD_ACCOUNT_D a
        LEFT JOIN DS.MD_LEDGER_ACCOUNT_S l 
            ON LEFT(a.account_number, 5) = l.chapter
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b 
            ON a.account_rk = b.account_rk AND b.on_date = CAST(%s AS DATE) - INTERVAL '1 day'
        LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F t 
            ON a.account_rk = t.account_rk 
            AND t.on_date BETWEEN DATE_TRUNC('month', CAST(%s AS DATE)) AND CAST(%s AS DATE) - INTERVAL '1 day'
        LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b_end 
            ON a.account_rk = b_end.account_rk AND b_end.on_date = CAST(%s AS DATE) - INTERVAL '1 day'

        WHERE a.data_actual_date <= CAST(%s AS DATE) 
          AND (a.data_actual_end_date IS NULL OR a.data_actual_end_date >= CAST(%s AS DATE))

        -- ✅ Исправленный GROUP BY для совместимости с агрегатными функциями
        GROUP BY 
            l.chapter, 
            LEFT(a.account_number, 5), 
            a.char_type;
    """

    # ✅ Исправленный список параметров (9 параметров для 9 плейсхолдеров %s)
    params = (
        i_OnDate, i_OnDate, i_OnDate,
        i_OnDate, i_OnDate, i_OnDate,
        i_OnDate, i_OnDate, i_OnDate
    )

    # Проверка количества плейсхолдеров и параметров
    count_placeholders = query.count('%s')
    print(f"\nКоличество плейсхолдеров %s в SQL: {count_placeholders}")
    print(f"Количество переданных параметров: {len(params)}")

    # ✅ Исправленный вызов SQL
    try:
        execute_query(query, params)
        print(f"\n✅ Успешный расчет 101 формы за {i_OnDate}\n")
    except Exception as e:
        print(f"\n❌ Ошибка при выполнении расчета 101 формы: {e}")


# Запуск расчета за январь 2018
def run_etl_for_january():
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2018, 1, 31)
    current_date = start_date

    while current_date <= end_date:
        fill_account_turnover_f(current_date)
        fill_account_balance_f(current_date)
        current_date += timedelta(days=1)

# Основной блок выполнения
if __name__ == "__main__":
    create_vitrines()                   # Создание таблиц и витрин
    fill_initial_balance()              # Заполнение остатков на 31.12.2017
    run_etl_for_january()               # Расчет витрин за январь 2018
    fill_f101_round_f('2018-02-01')     # Расчет 101 формы за январь 2018



