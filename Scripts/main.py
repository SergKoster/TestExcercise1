import sqlite3
import json
import Definitions

# Открываем json
with open("res/leads_response_1.json", "r", encoding="utf-8") as f:
    data1 = json.load(f)
with open("res/leads_response_2.json", "r", encoding="utf-8") as f:
    data2 = json.load(f)


conn = sqlite3.connect("Database/testExc.db") # инициализация бд
cursor = conn.cursor()

# Создание таблицы leads
Definitions.CreateLeadsTable(cursor)

Definitions.JsonToSQL(data1, cursor) # Перемещаем данные из json в sql таблицу
Definitions.JsonToSQL(data2, cursor)

Definitions.convert_to_csv(data1, cursor)



conn.commit()
conn.close()