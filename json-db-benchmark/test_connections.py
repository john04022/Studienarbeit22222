import mysql.connector
from pymongo import MongoClient
import oracledb

print("Teste MySQL...")
mysql_conn = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password="root",
    database="jsonbench"
)
cur = mysql_conn.cursor()
cur.execute("SELECT VERSION()")
print("MySQL:", cur.fetchone()[0])
cur.close()
mysql_conn.close()

print("Teste MongoDB...")
mongo = MongoClient("mongodb://root:root@127.0.0.1:27017/?authSource=admin")
print("MongoDB:", mongo.server_info()["version"])
mongo.close()

print("Teste Oracle...")
oracle_conn = oracledb.connect(
    user="system",
    password="Oracle12345",
    dsn="127.0.0.1:1521/FREEPDB1"
)
cur = oracle_conn.cursor()
cur.execute("SELECT banner FROM v$version")
print("Oracle:", cur.fetchone()[0])
cur.close()
oracle_conn.close()

print("Alle Verbindungen funktionieren.")
