import pathlib
import sqlite3
from threading import Lock

from src.settings import get_settings


class MetricsCollectorStub:
    def add_metric(self, name, value):
        pass

    def increment_metric(self, name, increment_value):
        pass

    def add_metrics_array(self, name, values, tags):
        pass

    def increment_metrics_array(self, name, increment_values, tags):
        pass

    def get_metric_by_name(self, name):
        return [], []


class MetricsCollector:
    __init_db = False
    __lock = Lock()

    def __init__(self, db_path=None):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.last_values = {}

        with MetricsCollector.__lock:
            if not MetricsCollector.__init_db:
                self._initialize_db()
                MetricsCollector.__init_db = True

    def __del__(self):
        self.conn.close()

    def _initialize_db(self):
        cur = self.conn.cursor()
        query = """
        CREATE TABLE IF NOT EXISTS metrics (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value REAL NOT NULL,
            tag TEXT DEFAULT NULL,
            timestamp DATETIME DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now'))
        );
        """
        cur.execute(query)
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_name_timestamp ON metrics(name, timestamp);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_tag ON metrics(tag) WHERE tag is not NULL;"
        )
        self.conn.commit()
        cur.close()

    def add_metric(self, name, value, tag=None):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO metrics (name, value, tag) VALUES (?, ?, ?)",
            (name, value, tag),
        )
        self.conn.commit()
        cur.close()

    def add_metrics_array(self, name, values, tags):
        for value, tag in zip(values, tags):
            self.add_metric(name, value, tag)

    def increment_metric(self, name, inc_value, tag=None):
        full_name = name
        if tag is not None:
            full_name = full_name + "_" + tag

        new_value = self.last_values.get(full_name, 0) + inc_value
        self.last_values[full_name] = new_value
        self.add_metric(name, new_value, tag)

    def increment_metrics_array(self, name, increment_values, tags):
        for inc_val, tag in zip(increment_values, tags):
            self.increment_metric(name, inc_val, tag)

    def get_metric_by_name(self, name):
        cur = self.conn.cursor()
        query = "SELECT value, timestamp FROM metrics WHERE name=?"
        cur.execute(query, (name,))
        results = cur.fetchall()
        cur.close()

        timestamps = [timestamp for _, timestamp in results]
        values = [value for value, _ in results]

        return timestamps, values

    def get_metric_by_tag_and_name(self, tag, name):
        cur = self.conn.cursor()
        query = "SELECT value, timestamp FROM metrics WHERE tag=? and name=?"
        cur.execute(query, (tag, name))
        results = cur.fetchall()
        cur.close()

        timestamps = [timestamp for _, timestamp in results]
        values = [value for value, _ in results]

        return timestamps, values

    def get_all_tags(self):
        cur = self.conn.cursor()

        query = "SELECT DISTINCT tag FROM metrics WHERE tag IS NOT NULL"
        cur.execute(query)

        tags = [row[0] for row in cur.fetchall()]

        self.conn.commit()
        cur.close()

        return tags

    def get_hosts(self):
        tags = self.get_all_tags()
        hosts = [tag for tag in tags if tag[:5] == "host="]
        return hosts


class MetricsCollectorFactory:
    db_path = None
    disable_metrics = True

    def __init__(self, db_path=None):
        settings = get_settings()

        if db_path is not None or "metrics_dir" in settings:
            MetricsCollectorFactory.disable_metrics = False

        if db_path is None and "metrics_dir" in settings:
            metrics_dir = settings["metrics_dir"]
            metrics_path = pathlib.Path(metrics_dir)
            metrics_path.mkdir(parents=True, exist_ok=True)

            db_path = metrics_dir + "/metrics.db"

        MetricsCollectorFactory.db_path = db_path

    @classmethod
    def initialize(cls):
        settings = get_settings()

        if "metrics_dir" in settings:
            metrics_dir = settings["metrics_dir"]
            metrics_path = pathlib.Path(metrics_dir)
            metrics_path.mkdir(parents=True, exist_ok=True)

            db_path = metrics_dir + "/metrics.db"

            cls.db_path = db_path
            cls.disable_metrics = False

    @classmethod
    def get_instance(cls, db_path=None):
        if db_path is not None:
            return MetricsCollector(db_path)

        if cls.disable_metrics:
            return MetricsCollectorStub()
        else:
            return MetricsCollector(cls.db_path)


MetricsCollectorFactory.initialize()


def get_metrics_collector(db_path=None):
    return MetricsCollectorFactory.get_instance(db_path)
