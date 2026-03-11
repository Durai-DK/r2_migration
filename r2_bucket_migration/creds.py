import os, mysql.connector, boto3, threading
from .log_settings import error_log, success_log
from botocore.config import Config
from mysql.connector import Error
from dotenv import load_dotenv
from pathlib import Path


env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)
thread_local = threading.local()


def r2_client():
    if not hasattr(thread_local, "client"):
        thread_local.client = boto3.client("s3",
                               endpoint_url=os.getenv("ENDPOINT"),
                               aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
                               aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
                               config=Config(signature_version="s3v4"),
                               region_name="auto")
    return thread_local.client


def mysql_connect():
    try:
        conn = mysql.connector.connect(host=os.getenv("HOST"),
                                       user=os.getenv("MYSQL_USER"),
                                       password=os.getenv("PASSWORD"),
                                       database=os.getenv("DATABASE"),
                                       port=3306)
        if conn.is_connected():
            success_log.info("✅ MySQL connection established")
            return conn
    except Error as e:
        error_log.error(f"❌ Error connecting to MySQL: {e}")
    return None


def fetch_mysql(last_pri_id, limit: int):
    conn = mysql_connect()
    if not conn:
        return []

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)

        query = """
                    SELECT *
                    FROM Transaction
                    WHERE pri_id > %s
                    ORDER BY pri_id ASC
                    LIMIT %s
                """

        cursor.execute(query, (last_pri_id, limit))
        return cursor.fetchall()

    except Error as e:
        error_log.error(f"❌ Batch Query Error: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
        conn.close()
