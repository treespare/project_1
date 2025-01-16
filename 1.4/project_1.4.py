import chardet
import psycopg2
import csv
import logging

from psycopg2 import sql

# Конфигурация базы данных
DB_CONFIG = {
    'dbname': 'bank_db',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5432',
    'options': '-c client_encoding=UTF8'
}

CSV_FILE_PATH = r"E:\Treespare\Рабочий стол\Новая папка (6)\Проект 1\1.4\f101_report.csv"

# Логирование
logging.basicConfig(filename=r"E:\Treespare\Рабочий стол\Новая папка (6)\Проект 1\1.4\etl_process.log",
                    level=logging.INFO, format='%(asctime)s - %(message)s')


def execute_query(query, params=None):
    """ Вспомогательная функция для выполнения SQL-запросов """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query, params or ())
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("SQL выполнен успешно.")
    except Exception as e:
        logging.error(f"Ошибка выполнения SQL: {e}")


def calculate_f101_report():
    """Расчет витрины DM_F101_ROUND_F с учетом исправлений"""
    query = """
    DELETE FROM DM.DM_F101_ROUND_F WHERE from_date = '2018-01-01';

    INSERT INTO DM.DM_F101_ROUND_F (
        from_date, to_date, chapter, ledger_account, characteristic, 
        balance_in_rub, balance_in_val, balance_in_total, 
        turn_deb_rub, turn_deb_val, turn_deb_total, 
        turn_cre_rub, turn_cre_val, turn_cre_total, 
        balance_out_rub, balance_out_val, balance_out_total, account_rk
    )
    SELECT 
        DATE_TRUNC('month', '2018-01-01'::DATE) AS from_date,  
        '2018-02-01'::DATE - INTERVAL '1 day' AS to_date,      
        COALESCE(l.chapter, 'Unknown') AS chapter,
        LEFT(a.account_number, 5) AS ledger_account,
        a.char_type AS characteristic,
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
        SUM(b_end.balance_out_rub) AS balance_out_total,
        a.account_rk
    FROM DS.MD_ACCOUNT_D a
    LEFT JOIN DS.MD_LEDGER_ACCOUNT_S l 
        ON LEFT(a.account_number, 5) = l.chapter
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b 
        ON a.account_rk = b.account_rk AND b.on_date = '2017-12-31'
    LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F t 
        ON a.account_rk = t.account_rk 
        AND t.on_date BETWEEN '2018-01-01' AND '2018-01-31'
    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b_end 
        ON a.account_rk = b_end.account_rk AND b_end.on_date = '2018-01-31'
    WHERE a.data_actual_date <= '2018-01-31'
      AND (a.data_actual_end_date IS NULL OR a.data_actual_end_date >= '2018-01-01')
    GROUP BY l.chapter, LEFT(a.account_number, 5), a.char_type, a.account_rk;
    """
    execute_query(query)
    logging.info("Витрина DM_F101_ROUND_F рассчитана.")


def export_to_csv(query, output_file):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        # Выполняем запрос для получения данных
        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        # Сохраняем данные в CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(colnames)  # Заголовки столбцов
            writer.writerows(rows)  # Данные

        print(f"Данные успешно экспортированы в {output_file}.")
    except Exception as e:
        print(f"Ошибка экспорта данных: {e}")
    finally:
        cur.close()
        conn.close()


def import_from_csv(input_file, table_name):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        with open(input_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)  # Заголовки столбцов
            rows = list(reader)

        # Подготовка SQL-запроса
        insert_query = sql.SQL(f"""
            INSERT INTO {table_name} ({', '.join(headers)})
            VALUES ({', '.join(['%s'] * len(headers))})
        """)

        # Обработка данных перед вставкой
        for row in rows:
            # Преобразование пустых строк в None
            row = [None if value == "" else value for value in row]
            cur.execute(insert_query, row)

        conn.commit()
        print(f"Данные успешно импортированы в таблицу {table_name}.")
        logging.info(f"Импорт данных из файла {input_file} в таблицу {table_name} завершён.")
    except Exception as e:
        logging.error(f"Ошибка импорта данных: {e}")
    finally:
        cur.close()
        conn.close()


def run_etl():
    """ Основной запуск ETL процесса """
    logging.info("Запуск ETL процесса...")

    # Расчёт 101 формы
    calculate_f101_report()

    # Экспорт данных в CSV
    export_to_csv(
        "SELECT * FROM DM.DM_F101_ROUND_F",  # SQL-запрос для экспорта данных
        r"E:\Treespare\Рабочий стол\Новая папка (6)\Проект 1\экспорт\dm_f101_round_f.csv"  # Путь для сохранения файла
    )

    # Импорт данных из CSV в таблицу DM_F101_ROUND_F_V2
    import_from_csv(
        r"E:\Treespare\Рабочий стол\Новая папка (6)\Проект 1\экспорт\dm_f101_round_f.csv",  # Путь к файлу
        "DM.DM_F101_ROUND_F_V2"  # Имя таблицы для импорта
    )

    logging.info("ETL процесс завершен успешно!")


# Запуск ETL
if __name__ == "__main__":
    # Экспорт данных
    export_to_csv(
        "SELECT * FROM DM.DM_F101_ROUND_F",
        r"E:\Treespare\Рабочий стол\Новая папка (6)\Проект 1\файлы\dm_f101_round_f.csv"
    )

    print("Измените файл CSV, затем продолжите выполнение скрипта.")

    # Импорт изменённых данных
    import_from_csv(
        r"E:\Treespare\Рабочий стол\Новая папка (6)\Проект 1\файлы\dm_f101_round_f.csv",
        "DM.DM_F101_ROUND_F_V2"
    )
