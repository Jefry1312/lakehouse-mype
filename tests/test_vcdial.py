from dotenv import load_dotenv
from pathlib import Path
import mysql.connector
import os

load_dotenv(Path(__file__).parent.parent / ".env")

def test_conexion(nombre, host, port, user, password, database):
    print(f"\n🔌 Probando conexión: {nombre}")
    try:
        conn = mysql.connector.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES;")
        tablas = cursor.fetchall()
        print(f"✅ Conexión exitosa - {len(tablas)} tablas encontradas")
        for tabla in tablas:
            print(f"   → {tabla[0]}")
        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")

test_conexion(
    nombre="VCDIAL",
    host=os.getenv("DB_HOST_VCDIAL"),
    port=os.getenv("DB_PORT_VCDIAL"),
    user=os.getenv("DB_USER_VCDIAL"),
    password=os.getenv("DB_PASSWORD_VCDIAL"),
    database=os.getenv("DB_NAME_VCDIAL")
)