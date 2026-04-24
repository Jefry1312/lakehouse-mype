from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

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

# Test SISGC
test_conexion(
    nombre="SISGC",
    host=os.getenv("DB_HOST_SISGC"),
    port=os.getenv("DB_PORT_SISGC"),
    user=os.getenv("DB_USER_SISGC"),
    password=os.getenv("DB_PASSWORD_SISGC"),
    database=os.getenv("DB_NAME_SISGC")  # ajusta el nombre de la variable si es diferente
)