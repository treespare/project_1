import psycopg2
from datetime import datetime, timedelta

# Параметры подключения к базе данных
DB_CONFIG = {
    'dbname': 'bank_db',
    'user': 'postgres',
    'password': 'password',
    'host': 'localhost',
    'port': '5432'
}


def calculate_dates(i_OnDate):
    """
    Рассчитывает ключевые даты для указанного отчетного периода.
    """
    report_date = datetime.strptime(i_OnDate, '%Y-%m-%d')
    from_date = (report_date - timedelta(days=report_date.day)).replace(day=1)
    to_date = from_date + timedelta(days=(from_date.replace(month=from_date.month + 1) - from_date).days - 1)
    prev_date = from_date - timedelta(days=1)
    return from_date, to_date, prev_date


def fill_f101_round_f(i_OnDate):
    """
    Рассчитывает 101 форму за отчетный период.
    """
    try:
        # Определение периодов
        from_date, to_date, prev_date = calculate_dates(i_OnDate)
        print(f"--- Начало расчета 101 формы за период: {from_date.date()} - {to_date.date()} ---")

        # Подключение к базе данных
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Удаление старых данных
                cur.execute("""
                    DELETE FROM DM.DM_F101_ROUND_F WHERE from_date = %s AND to_date = %s
                """, (from_date, to_date))
                print(f"✅ Удалены старые данные за {from_date} - {to_date}")

                # Выполнение расчета
                cur.execute("""
                    INSERT INTO DM.DM_F101_ROUND_F (
                        from_date, to_date, chapter, ledger_account, characteristic,
                        balance_in_rub, balance_in_val, balance_in_total,
                        turn_deb_rub, turn_deb_val, turn_deb_total,
                        turn_cre_rub, turn_cre_val, turn_cre_total,
                        balance_out_rub, balance_out_val, balance_out_total
                    )
                    SELECT 
                        %s AS from_date,
                        %s AS to_date,
                        COALESCE(TRIM(l.chapter), '') AS chapter,
                        LEFT(a.account_number, 5) AS ledger_account,
                        a.char_type AS characteristic,
                        -- Остатки на начало периода
                        SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_rub,
                        SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b.balance_out_rub ELSE 0 END) AS balance_in_val,
                        SUM(b.balance_out_rub) AS balance_in_total,
                        -- Обороты дебетовые
                        SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
                        SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.debet_amount_rub ELSE 0 END) AS turn_deb_val,
                        SUM(t.debet_amount_rub) AS turn_deb_total,
                        -- Обороты кредитовые
                        SUM(CASE WHEN a.currency_code IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
                        SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN t.credit_amount_rub ELSE 0 END) AS turn_cre_val,
                        SUM(t.credit_amount_rub) AS turn_cre_total,
                        -- Остатки на конец периода
                        SUM(CASE WHEN a.currency_code IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END) AS balance_out_rub,
                        SUM(CASE WHEN a.currency_code NOT IN ('810', '643') THEN b_end.balance_out_rub ELSE 0 END) AS balance_out_val,
                        SUM(b_end.balance_out_rub) AS balance_out_total
                    FROM DS.MD_ACCOUNT_D a
                    LEFT JOIN DS.MD_LEDGER_ACCOUNT_S l 
                        ON TRIM(LEFT(a.account_number, 5)) = TRIM(l.chapter)
                    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b 
                        ON a.account_rk = b.account_rk AND b.on_date = %s
                    LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F t 
                        ON a.account_rk = t.account_rk 
                        AND t.on_date BETWEEN %s AND %s
                    LEFT JOIN DM.DM_ACCOUNT_BALANCE_F b_end 
                        ON a.account_rk = b_end.account_rk AND b_end.on_date = %s
                    WHERE a.data_actual_date <= %s
                      AND (a.data_actual_end_date IS NULL OR a.data_actual_end_date >= %s)
                    GROUP BY 
                        l.chapter, 
                        LEFT(a.account_number, 5), 
                        a.char_type
                """, (from_date, to_date, prev_date, from_date, to_date, to_date, to_date, from_date))
                print(f"✅ Данные за период {from_date} - {to_date} успешно рассчитаны")

    except Exception as e:
        print(f"❌ Ошибка: {e}")


if __name__ == "__main__":
    fill_f101_round_f('2018-02-01')
