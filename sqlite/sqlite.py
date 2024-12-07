import sqlite3
import pandas as pd

def create_table(datanase_name: str, table_name: str) -> None:
    datanase_name = datanase_name.split('.')[0]
    conn = sqlite3.connect(f'./database/{datanase_name}.db')
    cursor = conn.cursor()
    cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
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

def add_to_db(df: pd.DataFrame, database_name: str, table_name: str, based_on: str='date') -> None:
    create_table(database_name, table_name)
    conn = sqlite3.connect(f'./database/{database_name}.db')
    cursor = conn.cursor()
    cursor.execute(f'SELECT date FROM {table_name}')
    existing_data = {row[0] for row in cursor.fetchall()}
    df['date'] = df['date'].astype(str)
    filltered_data = [tuple(row) for row in df.to_numpy() if row[0] not in existing_data]
    if filltered_data:
        cursor.executemany(f'''INSERT INTO {table_name} (date, open, high, low, close, volume)
                           VALUES (?, ?, ?, ?, ?, ?)''', filltered_data)
        conn.commit()
        
        first_date = min(row[0] for row in filltered_data)
        last_date = max(row[0] for row in filltered_data)
        
        print(f'{len(filltered_data)} data points added to {table_name} table from: {first_date} to {last_date}')
    else:
        print(f'No new data add to {table_name} table.')
    
    conn.close()

def remove_duplicates(database_name: str, table_name: str) -> None:
    conn = sqlite3.connect(f'./database/{database_name}.db')
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM {table_name}')
    before = cursor.fetchall()
    cursor.execute(f'DELETE FROM {table_name} where id not in (SELECT MIN(id) FROM data GROUP BY date)')
    conn.commit()
    cursor.execute(f'SELECT * FROM {table_name}')
    after = cursor.fetchall()
    conn.close()
    print(f'{len(before) - len(after)} rows deleted.')