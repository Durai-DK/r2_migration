import json, time, asyncio
from js import Response, JSON, console, Object
from datetime import datetime, timezone, date, timedelta


MAX_RETRIES = 5
BASE_BACKOFF = 0.25


def safe_to_py(obj):
    return obj.to_py() if hasattr(obj, "to_py") else obj


def deep_to_py(obj):
    if hasattr(obj, "to_py"):
        obj = obj.to_py()

    if isinstance(obj, dict):
        return {k: deep_to_py(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [deep_to_py(i) for i in obj]
    return obj


ALLOWED_ORIGINS = [
    "https://dev-ss-crm.poorvika.com",
    "https://stage-ss-crm.poorvika.com",
    "https://crm.poorvika.com",
    "http://localhost:3000",
]


def get_cors_headers(request):
    origin = (request.headers.get("Origin") or request.headers.get("origin") or "").strip()
    # console.log(f"🔍 CORS Check | Origin={origin} | Allowed={origin in ALLOWED_ORIGINS}")
    
    headers = {
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Token, X-Token-Id",
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Max-Age": "86400",
    }

    if origin in ALLOWED_ORIGINS:
        headers["Access-Control-Allow-Origin"] = origin

    return headers


def make_response(body, status=200, headers=None, request=None):
    if headers is None:
        headers = {}

    # 🔥 Inject dynamic CORS headers
    if request:
        cors_headers = get_cors_headers(request)
        for k, v in cors_headers.items():
            headers[k] = v

    if isinstance(body, (dict, list)):
        body = json.dumps(body)
        headers.setdefault("Content-Type", "application/json")

    init = JSON.parse(json.dumps({"status": status, "headers": headers}))
    return Response.new(body, init)

#--------------------------------------------------------------------

def get_date_paths():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")


def header_record_mapping(data):
    records = {
        "pri_id":"",
        "store_code__c": data.get("store_code", ""),
        "billed_at_branch_name": data.get("billed_at_branch_name", ""),
        "billed_at_company_name": data.get("billed_at_company_name", ""),
        "billed_at_address": data.get("billed_at_address", ""),
        "billed_at_addressline2": data.get("billed_at_addressline2", ""),
        "billed_at_addressline3": data.get("billed_at_addressline3", ""),
        "billed_at_city": data.get("billed_at_city", ""),
        "billed_at_pincode": data.get("billed_at_pincode", ""),
        "billed_at_state": data.get("billed_at_state", ""),
        "billed_at_phone_no1": data.get("billed_at_phone_no1", ""),
        "billed_at_phone_no2": data.get("billed_at_phone_no2", ""),
        "billed_at_GSTN_no": data.get("billed_at_GSTN_no", ""),
        "billed_at_state_code": data.get("billed_at_state_code", ""),
        "billed_at_PAN_no": data.get("billed_at_PAN_no", ""),
        "delivery_from_branch_store_code": data.get("delivery_from_branch_store_code", ""),
        "delivery_from_branch_name": data.get("delivery_from_branch_name", ""),
        "delivery_from_company_name": data.get("delivery_from_company_name", ""),
        "delivery_from_address": data.get("delivery_from_address", ""),
        "delivery_from_addressline2": data.get("delivery_from_addressline2", ""),
        "delivery_from_addressline3": data.get("delivery_from_addressline3", ""),
        "delivery_from_city": data.get("delivery_from_city", ""),
        "delivery_from_state": data.get("delivery_from_state", ""),
        "delivery_from_phone_no1": data.get("delivery_from_phone_no1", ""),
        "delivery_from_phone_no2": data.get("delivery_from_phone_no2", ""),
        "delivery_from_GSTN_no": data.get("delivery_from_GSTN_no", ""),
        "delivery_from_state_code": data.get("delivery_from_state_code", ""),
        "delivery_from_PAN_no": data.get("delivery_from_PAN_no", ""),
        "bill_refference_no": data.get("bill_refference_no", ""),
        "bill_refference_date": data.get("bill_refference_date", ""),
        "Bill_Date__c": data.get("bill_date", ""),
        "Bill_No__c": data.get("bill_no", ""),
        "bill_transaction_type__c": data.get("bill_transaction_type", ""),
        "bill_time__c": data.get("bill_time", ""),
        "bill_status__c": data.get("bill_status", ""),
        "bill_transaction_no__c": data.get("bill_transcation_no", ""),
        "bill_gross_amount__c": data.get("bill_item_gross_amount", ""),
        "bill_discount__c": data.get("bill_item_total_discount", ""),
        "bill_tax__c": data.get("bill_item_total_tax", ""),
        "bill_net_amount__c": data.get("bill_item_net_amount", ""),
        "bill_total_trade_deduction": data.get("bill_total_trade_deduction", ""),
        "bill_total_trade_addition": data.get("bill_total_trade_addition", ""),
        "Bill_Grant_Total__c": data.get("bill_grand_total", ""),
        "bill_cancel_date__c": data.get("bill_cancel_date", ""),
        "bill_cancel_time__c": data.get("bill_cancel_time", ""),
        "bill_cancel_amount__c": data.get("bill_cancel_amount", ""),
        "bill_modify_date": data.get("bill_modify_date", ""),
        "bill_modify_time": data.get("bill_modify_time", ""),
        "bill_remarks1__c": data.get("bill_remarks1", ""),
        "bill_remarks2__c": data.get("bill_remarks2", ""),
        "bill_tender_type__c": data.get("bill_tender_type", ""),
        "customer_fname__c": data.get("customer_fname", ""),
        "Customer_Number__c": data.get("customer_mobile", ""),
        "customer_email__c": data.get("customer_email", ""),
        "customer_dob__c": data.get("customer_dob", ""),
        "customer_doa__c": data.get("customer_doa", ""),
        "Customer_Code__c": data.get("customer_code", ""),
        "customer_lname__c": data.get("customer_lname", ""),
        "customer_gender__c": data.get("customer_gender", ""),
        "customer_city__c": data.get("customer_city", ""),
        "customer_area__c": data.get("customer_area", ""),
        "customer_address__c": data.get("customer_addressline1", ""),
        "customer_addressline2": data.get("customer_addressline2", ""),
        "customer_addressline3": data.get("customer_addressline3", ""),
        "customer_pincode": data.get("customer_pincode", ""),
        "customer_mobile__c": data.get("customer_phone_no1", ""),
        "customer_phone_no2": data.get("customer_phone_no2", ""),
        "customer_GSTN_no": data.get("customer_GSTN_no", ""),
        "customer_PAN_no": data.get("customer_PAN_no", ""),
        "customer_state__c": data.get("customer_state", ""),
        "customer_state_code": data.get("customer_state_code", ""),
        "delivery_to_Name": data.get("delivery_to_Name", ""),
        "delivery_to_addressline1": data.get("delivery_to_addressline1", ""),
        "delivery_to_addressline2": data.get("delivery_to_addressline2", ""),
        "delivery_to_addressline3": data.get("delivery_to_addressline3", ""),
        "delivery_to_city": data.get("delivery_to_city", ""),
        "delivery_to_state": data.get("delivery_to_state", ""),
        "delivery_to_pincode": data.get("delivery_to_pincode", ""),
        "delivery_to_phone_no1": data.get("delivery_to_phone_no1", ""),
        "delivery_to_phone_no2": data.get("delivery_to_phone_no2", ""),
        "delivery_to_GSTN_no": data.get("delivery_to_GSTN_no", ""),
        "delivery_to_state_code": data.get("delivery_to_state_code", ""),
        "delivery_to_PAN_no": data.get("delivery_to_PAN_no", ""),
        "created_At": "",
        "bill_datetime": data.get("bill_time", ""),
        'tid': '',
        'Id': '',
        'OwnerId': '',
        'IsDeleted': '',
        'Name': '',
        'CreatedDate': '',
        'CreatedById': '',
        'LastModifiedDate': '',
        'LastModifiedById': '',
        'SystemModstamp': '',
        'LastActivityDate': '',
        'Branch_Name__c': '',
        'Contact__c': '',
        'Customer__c': '',
        'Invoice_Amount__c': '',
        'Invoice_Date__c': '',
        'Customer_Last_Name__c': '',
        'Customer_Name__c': '',
        'Deptid': '',
        'billed_at_VAT_no': '',
        'delivery_from_VAT_no': '',
        'Email__c': '',
        'IMEINumber__c': '',
        'Item_Brand_Name__c': '',
        'Item_Group_Name__c': '',
        'Item_Rate__c': '',
        'Item_Remarks__c': '',
        'Location__c': '',
        'Product__c': '',
        'Products__c': '',
        'PurchasedDate__c': '',
        'Service_Center__c': '',
        'Showroom__c': '',
        'Showroom_code__c': '',
        'Status__c': '',
        'bill_cancel_against__c': '',
        'bill_cancel_reason__c': '',
        'bill_discount_per__c': '',
        'bill_grand_total__c': '',
        'bill_modify__c': '',
        'bill_modify_datetime__c': '',
        'bill_modify_reason__c': '',
        'bill_remarks3__c': '',
        'bill_remarks4__c': '',
        'bill_remarks5__c': '',
        'bill_round_off_amount__c': '',
        'bill_service_tax__c': '',
        'bill_type__c': '',
        'customer_remarks1__c': '',
        'customer_remarks2__c': '',
        'customer_remarks3__c': '',
        'customer_remarks4__c': '',
        'customer_remarks5__c': '',
        'ext_param1__c': '',
        'ext_param2__c': '',
        'ext_param3__c': '',
        'ext_param4__c': '',
        'ext_param5__c': '',
        'item_barcode__c': '',
        'item_brand_code__c': '',
        'item_category_code__c': '',
        'item_color_code__c': '',
        'item_color_name__c': '',
        'item_department_code__c': '',
        'item_department_name__c': '',
        'item_group__c': '',
        'item_remarks3__c': '',
        'item_remarks4__c': '',
        'item_remarks5__c': '',
        'item_service_tax__c': '',
        'item_size_code__c': '',
        'item_size_name__c': '',
        'item_sub_category_code__c': '',
        'item_sub_category_name__c': '',
        'voucher_code__c': '',
        'voucher_type__c': '',
        'voucher_value__c': '',
        'Docid__c': '',
        'Customer_Address_New__c': '',
        'customer_mobile__c_backup': '',
        'customer_state': '',
        'updated_At': '',
        'cusId_error': '',
        'customer_mobile__c2': '',
        'Customer_Code_New': '',
        'Customer_Code__c_new': '',
        'bill_remarks2': '',
        'customerId':'',
        'batch_id': '',
    }
    return records


def footer_record_mapping(data):
    records = {
        "item_brand_name": data.get("item_brand_name", ""),
        "item_category_name__c": data.get("item_category_name", ""),
        "item_cgst": data.get("item_cgst", ""),
        "item_cgst_perc": data.get("item_cgst_perc", ""),
        "Item_Code__c": data.get("item_code", ""),
        "item_discount__c": data.get("item_discount1", ""),
        "item_discount_per__c": data.get("item_discount2", ""),
        "item_gross_amount__c": data.get("item_gross_amount", ""),
        "item_gross_rate": data.get("item_gross_rate", ""),
        "item_igst": data.get("item_igst", ""),
        "item_igst_perc": data.get("item_igst_perc", ""),
        "item_name": data.get("item_name", ""),
        "Item_Name__c": data.get("item_name", ""),
        "item_net_amount__c": data.get("item_net_amount", ""),
        "item_product_name": data.get("item_product_name", ""),
        "item_quantity__c": data.get("item_quantity", ""),
        "item_sgst": data.get("item_sgst", ""),
        "item_sgst_perc": data.get("item_sgst_perc", ""),
        "item_sno": data.get("item_sno", ""),
        "item_status__c": data.get("item_status", ""),
        "item_tax__c": data.get("item_tax", ""),
        "item_taxable_Amount": data.get("item_taxable_Amount", ""),
        "item_remarks1__c": data.get("item_remarks1", ""),
        "item_remarks2__c": data.get("item_remarks2", ""),
        "emp_id": "",
        "emp_name": "",
        "ageOfDevice": None
    }
    return records


def verify_record_mapping(data):
    records = {
        "customer_mobile__c": data.get("customer_mobile__c"),
        "bill_transaction_no__c": data.get("bill_transaction_no__c"),
        "item_remarks1__c": data.get("item_remarks1__c"),
        "Item_Code__c": data.get("Item_Code__c"),
        "Bill_Date__c": data.get("Bill_Date__c"),
    }
    return records

#--------------------------------------------------------------------

async def store_json_data(env, data, key, metadata=None, bucket_binding="POS_BUCKET"):
    if not key:
        raise ValueError("R2 object key cannot be empty")

    body = json.dumps(data, ensure_ascii=False)

    delay = BASE_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            bucket = getattr(env, bucket_binding, None)

            if not bucket:
                raise ValueError(f"Bucket binding {bucket_binding} not found in env")

            # Convert metadata to JS Object
            metadata_js = Object.fromEntries([[k, str(v)] for k, v in (metadata or {}).items()])
            http_meta_js = Object.fromEntries([["contentType", "application/json"]])

            # Added httpMetadata for dashboard preview (Object Preview available)
            await bucket.put(key, body, customMetadata=metadata_js, httpMetadata=http_meta_js)
            return

        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            console.warn(
                f"[R2] ⚠️ Retry {attempt}/{MAX_RETRIES} | bucket={bucket_binding} | key={key}"
            )
            await asyncio.sleep(delay)
            delay *= 2


async def read_json_data(env, key, bucket_binding="POS_BUCKET"):
    try:
        bucket = getattr(env, bucket_binding, None)
        if not bucket:
            return None

        obj = await bucket.get(key)
        if not obj:
            return None

        text = await obj.text()
        return json.loads(text)
    except Exception as e:
        # Key not found logic usually returns null from get(), so exception means true error
        console.warn(f"⚠️ Failed to read {key} from {bucket_binding}: {e}")
        return None


def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

#--------------------------------------------------------------------

def parse_bill_date(item):
    date_str = item.get("Bill_Date__c")

    if not date_str:
        return datetime.min

    try:
        return datetime.fromisoformat(date_str)
    except (ValueError, TypeError):
        return datetime.min


def parse_date(inv_date):
    try:
        return datetime.strptime(inv_date.get("Bill_Date__c", ""), "%Y-%m-%d")
    except:
        return None


def get_invoice_year(inv):
    d = parse_bill_date(inv)
    return d.year if d else None


def normalize_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.strptime(value, "%Y-%m-%d")
    raise TypeError(f"Invalid date type: {type(value)}")

#--------------------------------------------------------------------