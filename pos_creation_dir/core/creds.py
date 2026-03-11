import os, mysql.connector, boto3, threading
from logs.log_settings import error_log
from botocore.client import Config
from mysql.connector import Error
from dotenv import load_dotenv
from pathlib import Path


env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

thread_local = threading.local()

def r2_client():
    endpoint = os.getenv("ENDPOINT")
    key_id = os.getenv("ACCESS_KEY_ID")
    secret_key = os.getenv("SECRET_ACCESS_KEY")

    if not hasattr(thread_local, "client"):
        thread_local.client = boto3.client("s3",
                               endpoint_url=endpoint,
                               aws_access_key_id=key_id,
                               aws_secret_access_key=secret_key,
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
            # success_log.info("✅ MySQL connection established")
            print("\n✅ MySQL connection established")
            return conn
    except Error as e:
        error_log.error(f"❌ Error connecting to MySQL: {e}")
    return None

def fetch_range(mobile):
    conn = mysql_connect()
    if not conn:
        return []

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)

        query = f"""
                    SELECT * FROM `Transaction`
                    WHERE customer_mobile__c = %s;
                """

        cursor.execute(query, (mobile,))
        return cursor.fetchall()

    except Error as e:
        error_log.error(f"❌ Query Error: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
        conn.close()

def fetch_batch_mobiles(mobile_list):
    conn = mysql_connect()
    if not conn:
        return []

    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)

        if not mobile_list:
            return []

        # Create placeholders for the IN clause
        placeholders = ', '.join(['%s'] * len(mobile_list))
        query = f"""
                    SELECT * FROM `Transaction`
                    WHERE customer_mobile__c IN ({placeholders});
                """

        cursor.execute(query, tuple(mobile_list))
        return cursor.fetchall()

    except Error as e:
        error_log.error(f"❌ Batch Query Error: {e}")
        return []

    finally:
        if cursor:
            cursor.close()
        conn.close()
