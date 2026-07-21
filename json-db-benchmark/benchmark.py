import argparse
import csv
import json
import statistics
import time
from pathlib import Path

import mysql.connector
from pymongo import MongoClient
import oracledb
import pandas as pd
import matplotlib.pyplot as plt


MYSQL_CONFIG = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "jsonbench"
}

MONGO_URI = "mongodb://root:root@127.0.0.1:27017/?authSource=admin"

ORACLE_DSN = "127.0.0.1:1521/FREEPDB1"
ORACLE_SYSTEM_USER = "system"
ORACLE_SYSTEM_PASSWORD = "Oracle12345"
ORACLE_USER = "BENCH"
ORACLE_PASSWORD = "Bench_12345"


def make_doc(i: int) -> dict:
    countries = ["DE", "FR", "US", "IT", "PL"]
    payments = ["Rechnung", "Kreditkarte", "PayPal", "Vorkasse"]
    articles = ["A-100", "A-200", "A-300", "A-400", "A-500"]

    return {
        "bestellnummer": f"B-{i:06d}",
        "status": "offen" if i % 3 != 0 else "abgeschlossen",
        "kunde": {
            "kundennummer": f"K-{10000 + i}",
            "name": f"Kunde {i}",
            "land": countries[i % len(countries)]
        },
        "positionen": [
            {
                "artikelnummer": articles[i % len(articles)],
                "menge": (i % 5) + 1,
                "preis": round(10 + (i % 50) * 0.75, 2)
            },
            {
                "artikelnummer": articles[(i + 2) % len(articles)],
                "menge": ((i + 1) % 5) + 1,
                "preis": round(5 + (i % 30) * 0.5, 2)
            }
        ],
        "zahlungsart": payments[i % len(payments)],
        "erstelltAm": f"2026-05-{(i % 28) + 1:02d}"
    }


def generate_docs(n: int) -> list[dict]:
    return [make_doc(i) for i in range(1, n + 1)]


def chunks(values, size):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def measure(system: str, operation: str, func, runs: int, warmups: int):
    values = []

    for i in range(warmups + runs):
        start = time.perf_counter()
        func()
        end = time.perf_counter()

        ms = (end - start) * 1000

        if i < warmups:
            print(f"{system:8} | {operation:24} | Warm-up: {ms:.3f} ms")
        else:
            print(f"{system:8} | {operation:24} | Lauf {i - warmups + 1:02d}: {ms:.3f} ms")
            values.append(ms)

    return {
        "system": system,
        "operation": operation,
        "avg_ms": statistics.mean(values),
        "median_ms": statistics.median(values),
        "min_ms": min(values),
        "max_ms": max(values)
    }


def mysql_benchmark(docs, runs, warmups, index_mode):
    print("\n=== MySQL ===")

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS bestellungen_json")

    if index_mode == "basic":
        cur.execute("""
            CREATE TABLE bestellungen_json (
                id INT AUTO_INCREMENT PRIMARY KEY,
                daten JSON NOT NULL,
                status_gen VARCHAR(30)
                    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(daten, '$.status'))) STORED,
                land_gen VARCHAR(5)
                    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(daten, '$.kunde.land'))) STORED,
                bestellnummer_gen VARCHAR(20)
                    GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(daten, '$.bestellnummer'))) STORED,
                INDEX idx_status (status_gen),
                INDEX idx_land (land_gen),
                INDEX idx_bestellnummer (bestellnummer_gen)
            )
        """)
    else:
        cur.execute("""
            CREATE TABLE bestellungen_json (
                id INT AUTO_INCREMENT PRIMARY KEY,
                daten JSON NOT NULL
            )
        """)

    conn.autocommit = False

    docs_json = [
        (json.dumps(d, ensure_ascii=False, separators=(",", ":")),)
        for d in docs
    ]

    def clear():
        conn.commit()
        cur.execute("TRUNCATE TABLE bestellungen_json")
        conn.commit()

    def insert_all():
        clear()
        sql = "INSERT INTO bestellungen_json (daten) VALUES (%s)"
        for batch in chunks(docs_json, 1000):
            cur.executemany(sql, batch)
        conn.commit()

    insert_all()

    if index_mode == "basic":
        q_status = "SELECT COUNT(*) FROM bestellungen_json WHERE status_gen = 'offen'"
        q_land = "SELECT COUNT(*) FROM bestellungen_json WHERE land_gen = 'DE'"
        q_update = """
            UPDATE bestellungen_json
            SET daten = JSON_SET(daten, '$.status', 'abgeschlossen')
            WHERE bestellnummer_gen = 'B-000001'
        """
        q_reset = """
            UPDATE bestellungen_json
            SET daten = JSON_SET(daten, '$.status', 'offen')
            WHERE bestellnummer_gen = 'B-000001'
        """
    else:
        q_status = """
            SELECT COUNT(*)
            FROM bestellungen_json
            WHERE JSON_UNQUOTE(JSON_EXTRACT(daten, '$.status')) = 'offen'
        """
        q_land = """
            SELECT COUNT(*)
            FROM bestellungen_json
            WHERE JSON_UNQUOTE(JSON_EXTRACT(daten, '$.kunde.land')) = 'DE'
        """
        q_update = """
            UPDATE bestellungen_json
            SET daten = JSON_SET(daten, '$.status', 'abgeschlossen')
            WHERE JSON_UNQUOTE(JSON_EXTRACT(daten, '$.bestellnummer')) = 'B-000001'
        """
        q_reset = """
            UPDATE bestellungen_json
            SET daten = JSON_SET(daten, '$.status', 'offen')
            WHERE JSON_UNQUOTE(JSON_EXTRACT(daten, '$.bestellnummer')) = 'B-000001'
        """

    q_array = """
        SELECT COUNT(*)
        FROM bestellungen_json
        WHERE JSON_SEARCH(daten, 'one', 'A-100', NULL, '$.positionen[*].artikelnummer') IS NOT NULL
    """

    def select_status():
        cur.execute(q_status)
        cur.fetchone()

    def select_land():
        cur.execute(q_land)
        cur.fetchone()

    def select_array():
        cur.execute(q_array)
        cur.fetchone()

    def update_status():
        cur.execute(q_reset)
        conn.commit()
        cur.execute(q_update)
        conn.commit()

    results = [
        measure("MySQL", "Insert", insert_all, runs, warmups),
        measure("MySQL", "Selektion Top-Level", select_status, runs, warmups),
        measure("MySQL", "Selektion verschachtelt", select_land, runs, warmups),
        measure("MySQL", "Selektion Array", select_array, runs, warmups),
        measure("MySQL", "Update", update_status, runs, warmups),
    ]

    cur.close()
    conn.close()
    return results


def mongodb_benchmark(docs, runs, warmups, index_mode):
    print("\n=== MongoDB ===")

    client = MongoClient(MONGO_URI)
    db = client["jsonbench"]

    def prepare_collection():
        db.drop_collection("bestellungen")
        c = db["bestellungen"]

        if index_mode == "basic":
            c.create_index("status")
            c.create_index("kunde.land")
            c.create_index("bestellnummer")
            c.create_index("positionen.artikelnummer")

        return c

    def insert_all():
        c = prepare_collection()
        for batch in chunks(docs, 1000):
            c.insert_many([d.copy() for d in batch], ordered=False)

    insert_all()
    col = db["bestellungen"]

    def select_status():
        col.count_documents({"status": "offen"})

    def select_land():
        col.count_documents({"kunde.land": "DE"})

    def select_array():
        col.count_documents({"positionen.artikelnummer": "A-100"})

    def update_status():
        col.update_one({"bestellnummer": "B-000001"}, {"$set": {"status": "offen"}})
        col.update_one({"bestellnummer": "B-000001"}, {"$set": {"status": "abgeschlossen"}})

    results = [
        measure("MongoDB", "Insert", insert_all, runs, warmups),
        measure("MongoDB", "Selektion Top-Level", select_status, runs, warmups),
        measure("MongoDB", "Selektion verschachtelt", select_land, runs, warmups),
        measure("MongoDB", "Selektion Array", select_array, runs, warmups),
        measure("MongoDB", "Update", update_status, runs, warmups),
    ]

    client.close()
    return results


def oracle_prepare_user():
    conn = oracledb.connect(
        user=ORACLE_SYSTEM_USER,
        password=ORACLE_SYSTEM_PASSWORD,
        dsn=ORACLE_DSN
    )
    conn.autocommit = True
    cur = conn.cursor()

    try:
        cur.execute("DROP USER BENCH CASCADE")
    except Exception:
        pass

    cur.execute(f'CREATE USER BENCH IDENTIFIED BY "{ORACLE_PASSWORD}" DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS')
    cur.execute("GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE TO BENCH")

    cur.close()
    conn.close()


def oracle_benchmark(docs, runs, warmups, index_mode):
    print("\n=== OracleDB ===")

    oracle_prepare_user()

    conn = oracledb.connect(
        user=ORACLE_USER,
        password=ORACLE_PASSWORD,
        dsn=ORACLE_DSN
    )
    conn.autocommit = False
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE bestellungen_json (
            id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            daten JSON
        )
    """)

    if index_mode == "basic":
        cur.execute("""
            CREATE INDEX idx_oracle_status
            ON bestellungen_json (
                JSON_VALUE(daten, '$.status' RETURNING VARCHAR2(30))
            )
        """)
        cur.execute("""
            CREATE INDEX idx_oracle_land
            ON bestellungen_json (
                JSON_VALUE(daten, '$.kunde.land' RETURNING VARCHAR2(5))
            )
        """)
        cur.execute("""
            CREATE INDEX idx_oracle_bestellnummer
            ON bestellungen_json (
                JSON_VALUE(daten, '$.bestellnummer' RETURNING VARCHAR2(20))
            )
        """)

    conn.commit()

    def clear():
        conn.commit()
        cur.execute("TRUNCATE TABLE bestellungen_json")
        conn.commit()

    def insert_all():
        clear()
        cur.setinputsizes(d=oracledb.DB_TYPE_JSON)
        sql = "INSERT INTO bestellungen_json (daten) VALUES (:d)"

        for batch in chunks(docs, 1000):
            cur.executemany(sql, [{"d": d} for d in batch])
        conn.commit()

    insert_all()

    q_status = """
        SELECT COUNT(*)
        FROM bestellungen_json
        WHERE JSON_VALUE(daten, '$.status' RETURNING VARCHAR2(30)) = 'offen'
    """

    q_land = """
        SELECT COUNT(*)
        FROM bestellungen_json
        WHERE JSON_VALUE(daten, '$.kunde.land' RETURNING VARCHAR2(5)) = 'DE'
    """

    q_array = """
        SELECT COUNT(DISTINCT b.id)
        FROM bestellungen_json b,
        JSON_TABLE(
            b.daten,
            '$.positionen[*]'
            COLUMNS (
                artikelnummer VARCHAR2(20) PATH '$.artikelnummer'
            )
        ) jt
        WHERE jt.artikelnummer = 'A-100'
    """

    q_reset = """
        UPDATE bestellungen_json
        SET daten = JSON_TRANSFORM(daten, SET '$.status' = 'offen')
        WHERE JSON_VALUE(daten, '$.bestellnummer' RETURNING VARCHAR2(20)) = 'B-000001'
    """

    q_update = """
        UPDATE bestellungen_json
        SET daten = JSON_TRANSFORM(daten, SET '$.status' = 'abgeschlossen')
        WHERE JSON_VALUE(daten, '$.bestellnummer' RETURNING VARCHAR2(20)) = 'B-000001'
    """

    def select_status():
        cur.execute(q_status)
        cur.fetchone()

    def select_land():
        cur.execute(q_land)
        cur.fetchone()

    def select_array():
        cur.execute(q_array)
        cur.fetchone()

    def update_status():
        cur.execute(q_reset)
        conn.commit()
        cur.execute(q_update)
        conn.commit()

    results = [
        measure("OracleDB", "Insert", insert_all, runs, warmups),
        measure("OracleDB", "Selektion Top-Level", select_status, runs, warmups),
        measure("OracleDB", "Selektion verschachtelt", select_land, runs, warmups),
        measure("OracleDB", "Selektion Array", select_array, runs, warmups),
        measure("OracleDB", "Update", update_status, runs, warmups),
    ]

    cur.close()
    conn.close()
    return results


def save_results(results, rows, runs, warmups, index_mode):
    Path("results").mkdir(exist_ok=True)
    Path("diagrams").mkdir(exist_ok=True)

    csv_path = Path("results") / f"json_benchmark_{rows}_{index_mode}.csv"

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["system", "operation", "avg_ms", "median_ms", "min_ms", "max_ms"],
            delimiter=";"
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"\nCSV gespeichert unter: {csv_path}")

    df = pd.DataFrame(results)
    pivot = df.pivot(index="operation", columns="system", values="avg_ms")

    ax = pivot.plot(kind="bar", figsize=(10, 6))
    ax.set_ylabel("Durchschnittliche Laufzeit in ms")
    ax.set_xlabel("Operation")
    ax.set_title(f"JSON-Benchmark: {rows} Dokumente, {runs} Messläufe, {warmups} Warm-up, Index: {index_mode}")
    plt.xticks(rotation=30, ha="right")
    plt.tight_layout()

    diagram_path = Path("diagrams") / f"json_benchmark_{rows}_{index_mode}.png"
    plt.savefig(diagram_path, dpi=200)
    plt.close()

    print(f"Diagramm gespeichert unter: {diagram_path}")

    print("\nLaTeX-Tabelle mit Durchschnittswerten:")
    print_latex_table(df, "avg_ms")

    print("\nLaTeX-Tabelle mit Median/Minimum/Maximum:")
    print_latex_stats_table(df)


def print_latex_table(df, value_column):
    pivot = df.pivot(index="operation", columns="system", values=value_column)

    order = [
        "Insert",
        "Selektion Top-Level",
        "Selektion verschachtelt",
        "Selektion Array",
        "Update"
    ]

    systems = ["OracleDB", "MongoDB", "MySQL"]

    print(r"\begin{table}[H]")
    print(r"\centering")
    print(r"\begin{tabular}{p{4cm}p{3cm}p{3cm}p{3cm}}")
    print(r"\hline")
    print(r"\textbf{Operation} & \textbf{OracleDB} & \textbf{MongoDB} & \textbf{MySQL} \\")
    print(r"\hline")

    for op in order:
        values = []
        for system in systems:
            if system in pivot.columns:
                values.append(f"{pivot.loc[op, system]:.2f} ms")
            else:
                values.append("-")
        print(f"{op} & {values[0]} & {values[1]} & {values[2]} \\\\")

    print(r"\hline")
    print(r"\end{tabular}")
    print(r"\caption{Durchschnittliche Laufzeiten der JSON-Operationen}")
    print(r"\label{tab:messwerte-json}")
    print(r"\end{table}")


def print_latex_stats_table(df):
    print(r"\begin{table}[H]")
    print(r"\centering")
    print(r"\begin{tabular}{p{4cm}p{3cm}p{3cm}p{3cm}p{3cm}}")
    print(r"\hline")
    print(r"\textbf{Operation} & \textbf{System} & \textbf{Median} & \textbf{Minimum} & \textbf{Maximum} \\")
    print(r"\hline")

    for _, row in df.iterrows():
        print(
            f"{row['operation']} & {row['system']} & "
            f"{row['median_ms']:.2f} ms & {row['min_ms']:.2f} ms & {row['max_ms']:.2f} ms \\\\"
        )

    print(r"\hline")
    print(r"\end{tabular}")
    print(r"\caption{Median, Minimum und Maximum der JSON-Operationen}")
    print(r"\label{tab:messwerte-statistik-json}")
    print(r"\end{table}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--index-mode", choices=["none", "basic"], default="none")
    parser.add_argument(
        "--systems",
        nargs="+",
        choices=["mysql", "mongodb", "oracle"],
        default=["mysql", "mongodb", "oracle"]
    )

    args = parser.parse_args()

    print(f"Erzeuge {args.rows} Testdokumente...")
    docs = generate_docs(args.rows)

    results = []

    if "mysql" in args.systems:
        results += mysql_benchmark(docs, args.runs, args.warmups, args.index_mode)

    if "mongodb" in args.systems:
        results += mongodb_benchmark(docs, args.runs, args.warmups, args.index_mode)

    if "oracle" in args.systems:
        results += oracle_benchmark(docs, args.runs, args.warmups, args.index_mode)

    save_results(results, args.rows, args.runs, args.warmups, args.index_mode)


if __name__ == "__main__":
    main()
