import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

try:
    conn = mysql.connector.connect(
        host     = os.getenv("DB_HOST"),
        port     = int(os.getenv("DB_PORT", 3306)),
        database = os.getenv("DB_NAME"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )

    cursor = conn.cursor()
    cursor.execute("SELECT @@hostname, @@port, DATABASE(), USER(), @@version")
    row = cursor.fetchone()

    print("✓ CONEXIÓN EXITOSA")
    print(f"  Servidor  : {row[0]}")
    print(f"  Puerto    : {row[1]}")
    print(f"  Base      : {row[2]}")
    print(f"  Usuario   : {row[3]}")
    print(f"  Versión   : {row[4]}")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"✗ ERROR DE CONEXIÓN: {e}")