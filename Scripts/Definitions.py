import sqlite3
import json
import random
import csv

def CreateLeadsTable(cursor):
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS leads(
                id INTEGER PRIMARY KEY,
                name TEXT,
                price DECIMAL(20, 2),
                responsible_user_id INTEGER,
                group_id INTEGER,
                status_id INTEGER,
                pipeline_id INTEGER,
                loss_reason_id INTEGER,
                created_by INTEGER,
                updated_by INTEGER,
                created_at INTEGER,
                updated_at INTEGER,
                closed_at INTEGER,
                closest_task_at INTEGER,
                is_deleted BIT,
                custom_fields TEXT,
                score TEXT,
                account_id INTEGER,
                labor_cost TEXT
            )""")
    
def JsonToSQL(data, cursor):

    leads = data["_embedded"]["leads"]

    for lead in leads:
        # Преобразуем custom_fields_values в JSON-строку
        custom_fields_json = json.dumps(lead.get("custom_fields_values", []))

        cursor.execute("""
            INSERT OR REPLACE INTO leads (id, name, price, responsible_user_id, group_id, status_id,
                            pipeline_id, loss_reason_id, created_by, updated_by,
                            created_at, updated_at, closed_at, is_deleted, custom_fields, score, account_id, labor_cost)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead["id"], lead["name"], lead["price"], lead["responsible_user_id"],
            lead["group_id"], lead["status_id"], lead["pipeline_id"],
            lead.get("loss_reason_id"), lead["created_by"], lead["updated_by"],
            lead["created_at"], lead["updated_at"], lead["closed_at"], lead["is_deleted"],
            custom_fields_json, lead["score"], lead["account_id"], lead["labor_cost"]
        ))


def api_emulator(data, page, page_size=10):
    """
    Эмулирует API: возвращает подмножество данных для заданной страницы.
    Например, если в исходном data в "_embedded" лежит список лидов,
    то возвращается только часть из них.
    """
    leads = data["_embedded"]["leads"]
    total = len(leads)
    total_pages = (total // page_size) + (1 if total % page_size else 0)
    start = (page - 1) * page_size
    end = start + page_size
    page_leads = leads[start:end]
    
    return {
        "_page": {
            "number": page,
            "size": page_size,
            "totalPages": total_pages,
            "totalElements": total
        },
        "_embedded": {
            "leads": page_leads
        }
    }




def convert_to_csv(data, cursor):
    all_leads = []
    # Симулируем 30 страниц; если данных меньше, цикл может завершиться раньше
    for page in range(1, 31):
        page_data = api_emulator(data, page, page_size=10)
        leads = page_data["_embedded"]["leads"]
        if not leads:  # Если страница пуста, можно выйти из цикла
            break
        all_leads.extend(leads)
        
        
        # JsonToSQL(page_data, cursor)
    
    # Выгрузка всех собранных данных в CSV
    with open("csv/output.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if all_leads:
            # Получаем заголовки из ключей первого лида
            headers = list(all_leads[0].keys())
            writer.writerow(headers)
            for lead in all_leads:
                row = [lead.get(col, "") for col in headers]
                writer.writerow(row)
    
    print("Выгрузка завершена, данные сохранены в output.csv")

    
    