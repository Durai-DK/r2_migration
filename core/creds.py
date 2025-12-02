import os
import boto3
import logging
import mysql.connector
from pathlib import Path
from dotenv import load_dotenv
from mysql.connector import Error
from fastapi import HTTPException
from botocore.client import Config
from pyiceberg.catalog.rest import RestCatalog

env_path = Path(__file__).resolve().parent / ".env"

load_dotenv(dotenv_path=env_path)

logger = logging.getLogger("r2_migrtaion")

def cat_client():
    cat_name = os.getenv("CATALOG_NAME")
    warehouse = os.getenv("WAREHOUSE")
    cat_uri = os.getenv("CATALOG_URI")
    cat_token = os.getenv("TOKEN")

    print(f"cat_name : {cat_name}\nwarehouse : {warehouse}\ncat_uri : {cat_uri}\ncat_token : {cat_token}")

    try:
        response = RestCatalog(name=cat_name, warehouse=warehouse, uri=cat_uri, token=cat_token)
        print(response)
    except Exception as e:
        logger.error(f"❌ Failed to initialize Iceberg catalog client: {e}")
        raise HTTPException(status_code=500, detail=f"Cloudflare R2 client 1 initialization failed: {e}")

def r2_client():
    endpoint = os.getenv("ENDPOINT")
    key_id = os.getenv("ACCESS_KEY_ID")
    secret_key = os.getenv("SECRET_ACCESS_KEY")

    try:
        return boto3.client("s3", endpoint_url=endpoint, aws_access_key_id=key_id,
                            aws_secret_access_key=secret_key,
                            config=Config(signature_version="s3v4"), region_name="auto")
    except Exception as e:
        logger.error(f"❌ Failed to initialize R2 client: {e}")
        raise HTTPException(status_code=500, detail="Cloudflare R2 client 2 initialization failed")

ALLOWED_TABLES = ["Transaction", "employees","POS_Transactions"]

def mysql_connect():
    try:
        conn = mysql.connector.connect(host=os.getenv("HOST"), user=os.getenv("MYSQL_USER"),
                                       password=os.getenv("PASSWORD"),
                                       database=os.getenv("DATABASE"), port=3306)
        if conn.is_connected():
            logger.info("✅ MySQL connection established")
            return conn
    except Error as e:
        logger.error(f"❌ Error connecting to MySQL: {e}")
    return None

class MysqlCatalog:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self):
        if not self.conn or not self.conn.is_connected():
            self.conn = mysql_connect()
            if self.conn:
                self.cursor = self.conn.cursor(dictionary=True)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _validate_table(self, table_name: str):
        if table_name not in ALLOWED_TABLES:
            raise ValueError(f"Invalid table name: {table_name}")

    def get_all_value(self, table_name):
        self._validate_table(table_name)
        self.connect()
        self.cursor.execute(f"SELECT * FROM `{table_name}`")
        return self.cursor.fetchall()

    def get_count(self, table_name: str):
        self._validate_table(table_name)
        self.connect()
        self.cursor.execute(f"SELECT COUNT(*) AS count FROM `{table_name}`")
        return self.cursor.fetchone()["count"]

    def get_describe(self, table_name: str):
        self._validate_table(table_name)
        self.connect()
        self.cursor.execute(f"DESCRIBE `{table_name}`")
        return self.cursor.fetchall()

    def get_range(self, table_name: str, start: int, end: int):
        self._validate_table(table_name)
        self.connect()
        # WHERE customer_mobile__c = 9500604848
        query = f"""
                SELECT * FROM `{table_name}`
                ORDER BY pri_id ASC
                LIMIT %s, %s
            """
        self.cursor.execute(query, (start, end - start))
        return self.cursor.fetchall()

    def get_transaction_range(self, table_name: str, start: int, end: int):
        self._validate_table(table_name)
        self.connect()
        query = f"""
            SELECT
                pri_id,
                store_code__c,
                Branch_Name__c,
                customerId,
                customer_mobile__c,
                Customer_Name__c,
                Bill_No__c,
                Bill_Date__c,
                Invoice_Amount__c,
                bill_status__c,
                bill_transaction_no__c,
                Item_Code__c,
                Item_Name__c,
                bill_tax__c,
                bill_grand_total__c,
                CreatedDate
            FROM `{table_name}`
            ORDER BY pri_id ASC
            LIMIT %s, %s
        """
        self.cursor.execute(query, (start, end - start))
        return self.cursor.fetchall()

    def get_one_pri_id(self, table_name: str, pri_id_value: int):
        self._validate_table(table_name)
        self.connect()
        query = f"""
            SELECT
                pri_id,
                store_code__c,
                Branch_Name__c,
                customer_mobile__c,
                Customer_Name__c,
                Bill_No__c,
                Bill_Date__c,
                Invoice_Date__c,
                Invoice_Amount__c,
                bill_status__c,
                bill_transaction_no__c,
                Item_Code__c,
                Item_Name__c,
                bill_tax__c,
                bill_grand_total__c,
                CreatedDate
            FROM `{table_name}`
            WHERE pri_id = %s
            LIMIT 1
        """
        self.cursor.execute(query, (pri_id_value,))
        return self.cursor.fetchone()

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
