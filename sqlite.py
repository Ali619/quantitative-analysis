import sqlite3
import pandas as pd

conn = sqlite3.connect('./database/price.db')

cursor = conn.cursor()

cursor.execute('''
               CREATE TABLE IF NOT EXISTS data (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   date DATE,
                   open REAL,
                   high REAL,
                   low REAL,
                   close REAL,
                   volume REAL
               )
               ''')

conn.commit()
conn.close()

# date,open,high,low,close,volume
# 2024-06-24 00:00:00,63209.26,63346.19,62688.24,62856.13,7712900.664065958

def add_to_db(df: pd.DataFrame):
    conn = sqlite3.connect('./database/price.db')
    cursor = conn.cursor()
    data = df.to_numpy()
    cursor.execute('SELECT * FROM data')
    existing_data = set(cursor.fetchall())
    filltered_data = [row for row in df.to_numpy() if row[0] not in existing_data]
    cursor.executemany('''INSERT INTO data (date, open, high, low, close, volume) 
                       VALUES (?, ?, ?, ?, ?, ?)''', filltered_data)    
    
    conn.commit()
    cursor.execute('SELECT * FROM data')
    print(len(cursor.fetchall()))
    conn.close()
df = pd.read_csv('./2024-12-07_18-17-4H-data.csv')
add_to_db(df)

def remove_duplicates():
    conn = sqlite3.connect('./database/price.db')
    cursor = conn.cursor()
    cursor.execute('DELETE FROM data where id not in (SELECT MIN(id) FROM data GROUP BY date)')
    conn.commit()
    
    cursor.execute('SELECT * FROM data')
    print(len(cursor.fetchall()))
    conn.close()
remove_duplicates()
