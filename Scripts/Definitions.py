import sqlite3
import json

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
                custom_fields TEXT
            )""")
    
def JsonToSQL(data, cursor):

    leads = data["_embedded"]["leads"]

    for lead in leads:
        # Преобразуем custom_fields_values в JSON-строку
        custom_fields_json = json.dumps(lead.get("custom_fields_values", []))

        cursor.execute("""
            INSERT INTO leads (id, name, price, responsible_user_id, group_id, status_id,
                            pipeline_id, loss_reason_id, created_by, updated_by,
                            created_at, updated_at, closed_at, is_deleted, custom_fields)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead["id"], lead["name"], lead["price"], lead["responsible_user_id"],
            lead["group_id"], lead["status_id"], lead["pipeline_id"],
            lead.get("loss_reason_id"), lead["created_by"], lead["updated_by"],
            lead["created_at"], lead["updated_at"], lead["closed_at"], lead["is_deleted"],
            custom_fields_json
        ))