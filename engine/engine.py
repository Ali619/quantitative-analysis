import sqlite3

import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import ConfusionMatrixDisplay, confusion_matrix, precision_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier


class Engine:
    def __init__(self, db_name: str, timeframe: str):
        self.rf = RandomForestClassifier()
        self.xgb = XGBClassifier()
        self._db_name = db_name
        self._timeframe = timeframe
        self._symbol = db_name.replace("_", "")
        self._table_name = self._set_table_name(timeframe)
        self._conn = None
        self._cursor = None

        self.connect_db()

    def connect_db(self) -> None:
        self._conn = sqlite3.connect(f"./database/{self._db_name}.db")
        self._cursor = self._conn.cursor()
        return None

    def change_db(self, db_name: str):
        if self._conn or self._cursor:
            self._cursor.close()
            self._conn.close()
            self._conn = None
            self._cursor = None

        if isinstance(db_name, str) and db_name:
            self._db_name = db_name
            self._conn = sqlite3.connect(f"./database/{self._db_name}.db")
            self._cursor = self._conn.cursor()
            print(f"Connected to {self._db_name}")
        else:
            raise ValueError("db_name must be a non-empty string")

    def get_db_info(self):
        return (self._db_name, self._table_name, self._symbol)

    @property
    def symbol(self) -> str:
        self._symbol = self._db_name.replace("_", "")
        return self._symbol

    @symbol.setter
    def symbol(self, symbol: str):
        """symbol must be a non-empty string, e.x `BTCUSDT`"""
        if isinstance(symbol, str) and symbol:
            self._symbol = symbol
        else:
            raise ValueError("symbol must be a non-empty string, e.x `BTCUSDT`")

    def _set_table_name(self, timeframe: str):
        assert (
            isinstance(timeframe, str) and timeframe
        ), "timeframe must be a non-empty string, e.x. `4h`"
        self._timeframe = timeframe
        self._table_name = self._symbol + "_" + self._timeframe
        return self._table_name

    def _fetch_data_db(self, timeframe: str):
        self._timeframe = timeframe
        self._table_name = self._symbol + "_" + self._timeframe
        self._cursor.execute(f"SELECT * FROM '{self._table_name}'")
        self.data = self._cursor.fetchall()
        return self.data

    def custom_query(self, db_name: str, timeframe: str, query: str):
        """NEED TO DEVELOP SPLIT THIS TO 3 FUNCTION FOR EACH COMMAND (SELECT, INSERT, DELETE).
        OR MAYBE CREATE A COMPLETE CLASS FOR QUERY COMMAND AND LEAVE THIS CLASS ONLY FOR MODEL TRAINING AND KKEP ONLY (SELECT * FROM ...) HERE
        """
        self._db_name = db_name
        self._table_name = self.table_name(timeframe)
        command = query.split(" ")[0]
        if command == "SELECT":
            self._cursor.execute(query)
            return self._cursor.fetchall()
        if command == "INSERT" or command == "DELETE":
            self._cursor.execute(query)
            self._conn.commit()
        self._cursor.close()
        self._conn.close()
        print("query is done")

    def preprocess_data(self, train_size: float = 0.8):
        split = int(len(self._fetch_data_db(self._symbol)) * train_size)
        self.df = pd.DataFrame(
            [row[1:] for row in self.data],  # Skip id col
            columns=["date", "open", "high", "low", "close", "volume"],
        )
        self.train_data = self.df[:split]
        self.test_data = self.df[split:]
        print(self.train_data.head())
        return self.train_data, self.test_data

    def run(self):
        self.connect_db()
        train_data, test_data = self.preprocess_data()


engine = Engine(db_name="BTC_USDT", timeframe="4h")
print(engine._fetch_data_db(timeframe="4h"))
