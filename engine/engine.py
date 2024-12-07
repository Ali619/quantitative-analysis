from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import precision_score, confusion_matrix, ConfusionMatrixDisplay
import xgboost
import sqlite3


class Engine:
    def __init__(self, db_name: str):
        self.rf = RandomForestClassifier()
        self.xgb = xgboost()
        self._db_name = db_name.split(".")[0]
        self._conn = None
        self._cursor = None
        self._symbol = None

    def db_connection(self) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
        self._conn = sqlite3.connect(f"./database/{self._db_name}.db")
        self._cursor = self._conn.cursor()
        return self._conn, self._cursor

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
        self._cursor.execute(f"SELECT * FROM ?", (self._symbol,))
        self.data = self._cursor.fetchall()
        return self.data

    def preprocess_data(self, symbol):
        split = len(self._fetch_data()) * 0.8
