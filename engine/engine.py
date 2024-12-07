import sqlite3

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import ConfusionMatrixDisplay, confusion_matrix, precision_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier


class Engine:
    def __init__(self, db_name: str, symbol: str):
        self.rf = RandomForestClassifier()
        self.xgb = XGBClassifier()
        self._db_name = db_name.split(".")[0]
        self._conn = None
        self._cursor = None
        self._symbol = symbol

    def connect_db(self) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        self._conn = sqlite3.connect(f"./database/{self._db_name}.db")
        self._cursor = self._conn.cursor()

    def change_db(self, db_name: str):
        if self._conn or self._cursor:
            self._conn.close()
            self._cursor.close()
            self._conn = None
            self._cursor = None

        if isinstance(db_name, str) and db_name:
            self._db_name = db_name.split(".")[0]
            self._conn = sqlite3.connect(f"./database/{self._db_name}.db")
            self._cursor = self._conn.cursor()
            print(f"Connected to {self._db_name}")
        else:
            raise ValueError("db_name must be a non-empty string")

    @property
    def symbol(self) -> str:
        return self._symbol

    @symbol.setter
    def symbol(self, symbol: str):
        if isinstance(symbol, str) and symbol:
            self._symbol = symbol
        else:
            raise ValueError("symbol must be a non-empty string, e.x `BTC_USDT`")

    def _fetch_data(self, symbol: str):
        self._symbol = symbol
        self._cursor.execute(f"SELECT * FROM {self._symbol}")
        self.data = self._cursor.fetchall()
        return self.data

    def preprocess_data(self, train_size: float = 0.8):
        split = int(len(self._fetch_data(self._symbol)) * 0.8)
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


engine = Engine(db_name="coinex_4h", symbol="BTC_USDT")
engine.run()
