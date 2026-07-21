import argparse
import csv
import json
import os
import statistics
import time
from datetime import datetime

import mysql.connector
from pymongo import MongoClient, ASCENDING
import oracledb


MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "jsonbench",
}

MONGO_URI = "mongodb://root:root@127.0.0.1:27017/?authSource=admin"
MONGO_DB = "jsonbench"

ORACLE_SYSTEM_CONFIG = {
    "user": "system",
    "password": "Oracle12345",
    "dsn": "127.0.0.1:1521/FREEPDB1",
}

ORACLE_BENCH_CONFIG = {
    "user": "BENCH",
    "password": "Bench_12345",
    "dsn": "127.0.0.1:1521/FREEPDB1",
}

TARGET_VALUE = "TREFFER"


def level_names(depth: int) -> list[str]:
    return [f"level{i:02d}" for i in range(1, depth + 1)]


def json_path(depth: int) -> str:
    return "$." + ".".join(level_names(depth)) + ".target"


def mongo_path(depth: int) -> str:
    return ".".join(level_names(depth) + ["target"])


def make_nested(depth: int, value: str) -> dict:
    node = {
        "target": value,
        "beschreibung": "Wert auf tief verschachtelter Ebene"
    }

    for level in reversed(level_names(depth)):
        node = {level: node}

    return node


def make_doc(i: int, depth: int) -> dict:
    countries = ["DE", "FR", "US", "IT", "PL"]
    payments = ["Rechnung", "Kreditkarte", "PayPal", "Vorkasse"]

    target = TARGET_VALUE if i % 10 == 0 else "KEIN_TREFFER"

    doc = {
        "_id": i,
        "bestellnummer": f"B-{i:06d}",
        "status": "offen" if i % 3 != 0 else "abgeschlossen",
        "kunde": {
            "kundennummer": f"K-{10000 + i}",
            "name": f"Kunde {i}",
            "land": countries[i % len(countries)]
        },
        "zahlungsart": payments[i % len(payments)],
        "erstelltAm": f"2026-05-{(i % 28) + 1:02d}",
        "tiefe": depth
    }

    doc.update(make_nested(depth, target))
    return doc


def generate_docs(rows: int, depth: int) -> list[dict]:
    return [make_doc(i, depth) for i in range(1, rows + 1)]


def measure(system: str, depth: int, operation: str, func, runs: int, warmups: int, index_mode: str, rows: int) -> dict:
    values = []
    last_result = None

    for i in range(warmups + runs):
        start = time.perf_counter()
        last_result = func()
        end = time.perf_counter()

        ms = (end - start) * 1000

        if i < warmups:
            print(f"{system:8} | Tiefe {depth:2} | {operation:22} | Warm-up: {ms:.3f} ms")
        else:
            values.append(ms)
            print(f"{system:8} | Tiefe {depth:2} | {operation:22} | Lauf {i - warmups + 1:02}: {ms:.3f} ms")

    return {
        "system": system,
        "depth": depth,
        "operation": operation,
        "index_mode": index_mode,
        "rows": rows,
        "runs": runs,
        "warmups": warmups,
        "avg_ms": statistics.mean(values),
        "median_ms": statistics.median(values),
        "min_ms": min(values),
        "max_ms": max(values),
        "last_result": last_result,
    }


def ensure_mysql_database():
    conn = mysql.connector.connect(
        host=MYSQL_CONFIG["host"],
        port=MYSQL_CONFIG["port"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
    )
    cur = conn.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS jsonbench")
    conn.commit()
    cur.close()
    conn.close()


def mysql_benchmark(rows: int, runs: int, warmups: int, depths: list[int], index_mode: str) -> list[dict]:
    ensure_mysql_database()
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor()
    results = []

    for depth in depths:
        table = f"depth_json_d{depth}"
        path = json_path(depth)

        cur.execute(f"DROP TABLE IF EXISTS {table}")

        if index_mode == "basic":
            cur.execute(f"""
                CREATE TABLE {table} (
                    doc_id INT PRIMARY KEY,
                    daten JSON NOT NULL,
                    target_gen VARCHAR(50)
                        GENERATED ALWAYS AS (
                            JSON_UNQUOTE(JSON_EXTRACT(daten, '{path}'))
                        ) STORED,
                    INDEX idx_target_gen (target_gen)
                )
            """)
        else:
            cur.execute(f"""
                CREATE TABLE {table} (
                    doc_id INT PRIMARY KEY,
                    daten JSON NOT NULL
                )
            """)

        conn.commit()

        def insert_all():
            docs = generate_docs(rows, depth)
            cur.execute(f"DELETE FROM {table}")
            cur.executemany(
                f"INSERT INTO {table} (doc_id, daten) VALUES (%s, %s)",
                [(d["_id"], json.dumps(d, ensure_ascii=False, separators=(",", ":"))) for d in docs]
            )
            conn.commit()
            return rows

        update_counter = {"value": 0}

        def select_deep():
            if index_mode == "basic":
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE target_gen = %s", (TARGET_VALUE,))
            else:
                cur.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM {table}
                    WHERE JSON_UNQUOTE(JSON_EXTRACT(daten, %s)) = %s
                    """,
                    (path, TARGET_VALUE)
                )
            return cur.fetchone()[0]

        def update_deep():
            update_counter["value"] += 1
            new_value = f"UPDATED_{update_counter['value']}"
            cur.execute(
                f"""
                UPDATE {table}
                SET daten = JSON_SET(daten, %s, %s)
                WHERE doc_id = 1
                """,
                (path, new_value)
            )
            conn.commit()
            return cur.rowcount

        print(f"\n=== MySQL | Tiefe {depth} | Index: {index_mode} ===")
        results.append(measure("MySQL", depth, "Insert", insert_all, runs, warmups, index_mode, rows))

        insert_all()
        expected = rows // 10
        actual = select_deep()
        if actual != expected:
            print(f"WARNUNG MySQL Tiefe {depth}: Erwartet {expected}, gefunden {actual}")

        results.append(measure("MySQL", depth, "Selektion tiefer Pfad", select_deep, runs, warmups, index_mode, rows))
        results.append(measure("MySQL", depth, "Update tiefer Pfad", update_deep, runs, warmups, index_mode, rows))

    cur.close()
    conn.close()
    return results


def mongo_benchmark(rows: int, runs: int, warmups: int, depths: list[int], index_mode: str) -> list[dict]:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    results = []

    for depth in depths:
        coll_name = f"depth_json_d{depth}"
        path = mongo_path(depth)

        db.drop_collection(coll_name)
        coll = db[coll_name]

        if index_mode == "basic":
            coll.create_index([(path, ASCENDING)], name="idx_target_path")

        def insert_all():
            coll.delete_many({})
            docs = generate_docs(rows, depth)
            coll.insert_many(docs, ordered=False)
            return rows

        update_counter = {"value": 0}

        def select_deep():
            return coll.count_documents({path: TARGET_VALUE})

        def update_deep():
            update_counter["value"] += 1
            new_value = f"UPDATED_{update_counter['value']}"
            result = coll.update_one(
                {"_id": 1},
                {"$set": {path: new_value}}
            )
            return result.modified_count

        print(f"\n=== MongoDB | Tiefe {depth} | Index: {index_mode} ===")
        results.append(measure("MongoDB", depth, "Insert", insert_all, runs, warmups, index_mode, rows))

        insert_all()
        expected = rows // 10
        actual = select_deep()
        if actual != expected:
            print(f"WARNUNG MongoDB Tiefe {depth}: Erwartet {expected}, gefunden {actual}")

        results.append(measure("MongoDB", depth, "Selektion tiefer Pfad", select_deep, runs, warmups, index_mode, rows))
        results.append(measure("MongoDB", depth, "Update tiefer Pfad", update_deep, runs, warmups, index_mode, rows))

    client.close()
    return results


def ensure_oracle_user():
    conn = oracledb.connect(**ORACLE_SYSTEM_CONFIG)
    cur = conn.cursor()

    block = """
    DECLARE
        v_count NUMBER;
    BEGIN
        SELECT COUNT(*) INTO v_count
        FROM all_users
        WHERE username = 'BENCH';

        IF v_count = 0 THEN
            EXECUTE IMMEDIATE 'CREATE USER BENCH IDENTIFIED BY Bench_12345 QUOTA UNLIMITED ON USERS';
        END IF;

        EXECUTE IMMEDIATE 'GRANT CONNECT, RESOURCE, CREATE VIEW TO BENCH';
    END;
    """
    cur.execute(block)
    conn.commit()
    cur.close()
    conn.close()


def oracle_drop_table(cur, table: str):
    try:
        cur.execute(f"DROP TABLE {table} PURGE")
    except Exception:
        pass


def oracle_benchmark(rows: int, runs: int, warmups: int, depths: list[int], index_mode: str) -> list[dict]:
    ensure_oracle_user()

    conn = oracledb.connect(**ORACLE_BENCH_CONFIG)
    cur = conn.cursor()
    results = []

    for depth in depths:
        table = f"DEPTH_JSON_D{depth}"
        path = json_path(depth)

        oracle_drop_table(cur, table)
        cur.execute(f"""
            CREATE TABLE {table} (
                doc_id NUMBER PRIMARY KEY,
                daten JSON
            )
        """)

        if index_mode == "basic":
            cur.execute(f"""
                CREATE INDEX IDX_{table}_TARGET
                ON {table} (
                    JSON_VALUE(daten, '{path}' RETURNING VARCHAR2(50))
                )
            """)

        conn.commit()

        def insert_all():
            docs = generate_docs(rows, depth)
            cur.execute(f"DELETE FROM {table}")
            cur.executemany(
                f"INSERT INTO {table} (doc_id, daten) VALUES (:id, JSON(:d))",
                [
                    {
                        "id": d["_id"],
                        "d": json.dumps(d, ensure_ascii=False, separators=(",", ":"))
                    }
                    for d in docs
                ]
            )
            conn.commit()
            return rows

        update_counter = {"value": 0}

        def select_deep():
            cur.execute(
                f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE JSON_VALUE(daten, '{path}' RETURNING VARCHAR2(50)) = :v
                """,
                {"v": TARGET_VALUE}
            )
            return cur.fetchone()[0]

        def update_deep():
            update_counter["value"] += 1
            new_value = f"UPDATED_{update_counter['value']}"
            cur.execute(
                f"""
                UPDATE {table}
                SET daten = JSON_TRANSFORM(daten, SET '{path}' = :v)
                WHERE doc_id = 1
                """,
                {"v": new_value}
            )
            conn.commit()
            return cur.rowcount

        print(f"\n=== OracleDB | Tiefe {depth} | Index: {index_mode} ===")
        results.append(measure("OracleDB", depth, "Insert", insert_all, runs, warmups, index_mode, rows))

        insert_all()
        expected = rows // 10
        actual = select_deep()
        if actual != expected:
            print(f"WARNUNG OracleDB Tiefe {depth}: Erwartet {expected}, gefunden {actual}")

        results.append(measure("OracleDB", depth, "Selektion tiefer Pfad", select_deep, runs, warmups, index_mode, rows))
        results.append(measure("OracleDB", depth, "Update tiefer Pfad", update_deep, runs, warmups, index_mode, rows))

    cur.close()
    conn.close()
    return results


def save_results(results: list[dict]) -> str:
    os.makedirs("results", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"results/depth_benchmark_{timestamp}.csv"

    fieldnames = [
        "system",
        "depth",
        "operation",
        "index_mode",
        "rows",
        "runs",
        "warmups",
        "avg_ms",
        "median_ms",
        "min_ms",
        "max_ms",
        "last_result",
    ]

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        writer.writeheader()
        writer.writerows(results)

    return file_path


def print_summary(results: list[dict]):
    print("\n=== Zusammenfassung ===")
    for r in results:
        print(
            f"{r['system']:8} | Tiefe {r['depth']:2} | {r['operation']:22} | "
            f"avg={r['avg_ms']:.3f} ms | median={r['median_ms']:.3f} ms | "
            f"min={r['min_ms']:.3f} ms | max={r['max_ms']:.3f} ms"
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--depths", nargs="+", type=int, default=[1, 2, 4, 6, 8])
    parser.add_argument("--index-mode", choices=["none", "basic"], default="none")
    parser.add_argument("--systems", nargs="+", choices=["mysql", "mongodb", "oracle"], default=["mysql", "mongodb", "oracle"])
    args = parser.parse_args()

    all_results = []

    if "mysql" in args.systems:
        all_results.extend(mysql_benchmark(args.rows, args.runs, args.warmups, args.depths, args.index_mode))

    if "mongodb" in args.systems:
        all_results.extend(mongo_benchmark(args.rows, args.runs, args.warmups, args.depths, args.index_mode))

    if "oracle" in args.systems:
        all_results.extend(oracle_benchmark(args.rows, args.runs, args.warmups, args.depths, args.index_mode))

    print_summary(all_results)
    file_path = save_results(all_results)
    print(f"\nErgebnisse gespeichert unter: {file_path}")


if __name__ == "__main__":
    main()
