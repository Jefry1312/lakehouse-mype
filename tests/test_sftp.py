from dotenv import load_dotenv
from pathlib import Path
import paramiko
import os

load_dotenv(Path(__file__).parent.parent / ".env")

def test_sftp():
    print("\n🔌 Probando conexión SFTP...")
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(
            hostname=os.getenv("SFTP_HOST"),
            port=int(os.getenv("SFTP_PORT")),
            username=os.getenv("SFTP_USER"),
            password=os.getenv("SFTP_PASS")
        )
        sftp = ssh.open_sftp()
        archivos = sftp.listdir(".")
        print(f"✅ Conexión exitosa - {len(archivos)} archivos encontrados")
        for archivo in archivos:
            print(f"   → {archivo}")
        sftp.close()
        ssh.close()
    except Exception as e:
        print(f"❌ Error: {e}")

test_sftp()