import pathlib
import sqlite3
from threading import Lock

from src.settings import get_settings


class MetricsCollectorStub:
    @staticmethod
    def _initialize_db():
        pass

    def add_metric(self, name, value):
        pass

    def increment_metric(self, name, increment_value):
        pass

    def get_metric(self, name):
        return [], []


class MetricsCollector:
    __instance = None
    __lock = Lock()

    def __new__(cls, db_path=None, disable_metrics=True):
        settings = get_settings()
        if db_path is not None or "metrics_dir" in settings:
            disable_metrics = False

        if db_path is None and "metrics_dir" in settings:
            metrics_dir = settings["metrics_dir"]
            metrics_path = pathlib.Path(metrics_dir)
            metrics_path.mkdir(parents=True, exist_ok=True)

            db_path = metrics_dir + "/metrics.db"

        with cls.__lock:
            if cls.__instance is None:
                if disable_metrics:
                    cls.__instance = MetricsCollectorStub()
                    return cls.__instance

                cls.__instance = super().__new__(cls)
                cls.__instance.db_path = db_path
                cls.__instance.conn = sqlite3.connect(db_path, check_same_thread=False)
                cls.__instance.last_values = {}
                cls.__instance._initialize_db()
            return cls.__instance

    def __del__(self):
        self.conn.close()

    @staticmethod
    def _initialize_db():
        cur = MetricsCollector.__instance.conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL NOT NULL,
            timestamp DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
        );
        """
        cur.execute(query)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_name_timestamp ON metrics(name, timestamp);"
        )
        MetricsCollector.__instance.conn.commit()
        cur.close()

    def add_metric(self, name, value):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO metrics (name, value) VALUES (?, ?)", (name, value))
        self.conn.commit()
        cur.close()

    def increment_metric(self, name, increment_value):
        new_value = self.last_values.get(name, 0) + increment_value
        self.last_values[name] = new_value
        self.add_metric(name, new_value)

    def get_metric(self, name):
        cur = self.conn.cursor()
        query = "SELECT value, timestamp FROM metrics WHERE name=?"
        cur.execute(query, (name,))
        results = cur.fetchall()
        cur.close()

        timestamps = [timestamp for _, timestamp in results]
        values = [value for value, _ in results]

        return timestamps, values
