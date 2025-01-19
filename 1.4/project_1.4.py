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

CSV_FILE_PATH = r"E:/Treespare/Рабочий стол/Новая папка (6)/Проект 1/файлы/dm_f101_round_f.csv"

# Логирование
logging.basicConfig(filename=r"E:/Treespare/Рабочий стол/Новая папка (6)/Проект 1/1.4/etl_process.log",
                    level=logging.INFO, format='%(asctime)s - %(message)s')

def export_to_csv(query, output_file):
    """Экспорт данных в CSV"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(colnames)
            writer.writerows(rows)

        logging.info(f"Данные успешно экспортированы в {output_file}.")
        print(f"Данные успешно экспортированы в {output_file}.")
    except Exception as e:
        logging.error(f"Ошибка экспорта данных: {e}")
        print(f"Ошибка экспорта данных: {e}")
    finally:
        cur.close()
        conn.close()

def import_from_csv(input_file, table_name):
    """Импорт данных из CSV"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        with open(input_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            headers = next(reader)  # Считываем заголовки столбцов
            rows = list(reader)

        insert_query = sql.SQL(f"""
            INSERT INTO {table_name} ({', '.join(headers)})
            VALUES ({', '.join(['%s'] * len(headers))})
        """)

        for row in rows:
            row = [None if value == "" else value for value in row]  # Преобразование пустых значений
            cur.execute(insert_query, row)

        conn.commit()
        logging.info(f"Данные успешно импортированы в таблицу {table_name}.")
        print(f"Данные успешно импортированы в таблицу {table_name}.")
    except Exception as e:
        logging.error(f"Ошибка импорта данных: {e}")
        print(f"Ошибка импорта данных: {e}")
    finally:
        cur.close()
        conn.close()

def run_etl():
    """Основной процесс ETL"""
    logging.info("Запуск ETL процесса...")

    # Экспорт данных из витрины в CSV
    export_to_csv(
        "SELECT * FROM DM.DM_F101_ROUND_F",  # SQL-запрос для экспорта данных
        CSV_FILE_PATH  # Путь для сохранения CSV файла
    )

    print("Измените файл CSV, затем продолжите выполнение скрипта.")

    # Импорт данных из CSV в копию таблицы
    import_from_csv(
        CSV_FILE_PATH,  # Путь к файлу
        "DM.DM_F101_ROUND_F_V2"  # Имя таблицы для импорта
    )

    logging.info("ETL процесс завершен успешно!")
    print("ETL процесс завершен успешно!")

if __name__ == "__main__":
    run_etl()
