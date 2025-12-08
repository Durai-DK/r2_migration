import requests, os, json
from core.creds import r2_client
from datetime import datetime, date
from typing import Any, Dict, Optional
from fastapi.responses import JSONResponse
from logs.log_settings import error_log, success_log


API_CRM_BASE_URL = os.getenv("API_CRM_BASE_URL", "")
API_CRM_AUTH_TOKEN = os.getenv("API_CRM_AUTH_TOKEN", "")

age_param = {"CompanyCode": "pmpl"}

age_header = {"securitycode": "5382-1294-8763-3537", "userid": "poorvika"}

age_url = "https://apx.poorvikamobiles.net/api/apxapi/GetImeiSerialNoCurrentStatus"

pos_bucket = "pos-transaction"
pos_imei_bucket = "pos-transaction-imei"
mobile_prefix = "mobile"
imei_prefix = "imei_numbers"

def error_response(code: int, message: str, error: Any):
    return JSONResponse(
        status_code=code,
        content={"status": "failed", "message": message, "error": error},
    )

def safe_json_dumps(data: Any) -> str:
    if isinstance(data, (dict, list, str, int, float)):
        return json.dumps(data, indent=2, ensure_ascii=False)
    raise TypeError(f"Unsupported JSON type: {type(data)}")

def store_json_data(bucket: str, data: Any, key: str, metadata: Optional[Dict[str, str]] = None):
    if not key:
        raise ValueError("R2 object key cannot be empty")

    body = safe_json_dumps(data)
    r2 = r2_client()

    try:
        r2.put_object(Bucket=bucket,
                      Key=key,
                      Body=body.encode("utf-8"),
                      ContentType="application/json",
                      Metadata=metadata or {})

        success_log.info(f"✅ Stored R2 Bucket | Key: {key} ")

    except Exception as e:
        error_log.exception(f"❌ Failed to store R2 data {key}: {e}")
        raise

def header_record_mapping(data):
    records = {
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
        'item_serial_no__c': '',
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
    }
    return records

def footer_record_mapping(data):
    records = {
        "item_sno": data.get("item_sno", ""),
        "Item_Code__c": data.get("item_code", ""),
        "item_name": data.get("item_name", ""),
        "Item_Name__c": data.get("item_name", ""),
        "item_gross_rate": data.get("item_gross_rate", ""),
        "item_gross_amount__c": data.get("item_gross_amount", ""),
        "item_quantity__c": data.get("item_quantity", ""),
        "item_discount__c": data.get("item_discount1", ""),
        "item_discount_per__c": data.get("item_discount2", ""),
        "item_taxable_Amount": data.get("item_taxable_Amount", ""),
        "item_tax__c": data.get("item_tax", ""),
        "item_net_amount__c": data.get("item_net_amount", ""),
        "item_brand_name": data.get("item_brand_name", ""),
        "item_category_name__c": data.get("item_category_name", ""),
        "item_product_name": data.get("item_product_name", ""),
        "item_status__c": data.get("item_status", ""),
        "item_cgst_perc": data.get("item_cgst_perc", ""),
        "item_cgst": data.get("item_cgst", ""),
        "item_sgst_perc": data.get("item_sgst_perc", ""),
        "item_sgst": data.get("item_sgst", ""),
        "item_igst_perc": data.get("item_igst_perc", ""),
        "item_igst": data.get("item_igst", ""),
        "item_remarks2__c": data.get("item_remarks2", ""),
    }
    return records

def fetch_age_of_device(serial_no: str):
    if not serial_no:
        return None

    age_param["ImeiSerialNo"] = serial_no

    try:
        response = requests.get(age_url, headers=age_header, params=age_param).json()
    except Exception as e:
        error_log.exception(f"❌ Age-of-device API failed : {str(e)}")
        return None

    # ---- Validate API Status ----
    if response.get("StatusCode") != 0:
        error_log.warning(f"⚠️ Age API error: {response.get('StatusMessage')}")
        return None

    # ---- Validate Data list ----
    data = response.get("Data") or []
    inward_raw = data[0].get("INWARD_DATE") if data else None
    if not inward_raw:
        return None

    try:
        inward_date = datetime.strptime(str(inward_raw), "%Y%m%d").date()
        return (date.today() - inward_date).days
    except Exception as e:
        error_log.exception(f"❌ Failed parsing INWARD_DATE : {str(e)}")
        return None

def crm_customer_id(request_body):
    try:
        current_data = request_body

        data = {
            "name": current_data.get("customer_fname")
                    or current_data.get("customer_lname"),

            "mobile": current_data.get("customer_mobile")
                      or current_data.get("customer_phone_no1")
                      or current_data.get("customer_phone_no2"),

            "gender": current_data.get("customer_gender"),
            "email": current_data.get("customer_email__c"),
            "type": "Customer",

            "pincode": current_data.get("customer_pincode")
                       or current_data.get("billed_at_pincode")
                       or current_data.get("delivery_to_pincode"),

            "branch": [current_data.get("store_code")],

            "city": current_data.get("customer_city")
                    or current_data.get("billed_at_city")
                    or current_data.get("delivery_to_city"),

            "state": current_data.get("customer_state")
                     or current_data.get("billed_at_state")
                     or current_data.get("delivery_from_state"),

            "alt_mobile": current_data.get("customer_phone_no1")
                          or current_data.get("customer_phone_no2"),

            "source": "Python",
            "status": True
        }

        headers = {
            "Authorization": API_CRM_AUTH_TOKEN,
            "Content-Type": "application/json"
        }

        resp = requests.post(API_CRM_BASE_URL, json=data, headers=headers).json()

        if resp.get("status") in (200, 201):
            return resp.get("data", {}).get("customer_id", ""), None

        if resp.get("status") == 400:
            return resp.get("errors", [{}])[0].get("customerId", ""), None

        return "", {"stage": "crm", "response": resp}

    except Exception as e:
        error_log.exception("❌ CRM exception")
        return "", {"stage": "crm_exception", "message": str(e)}

def build_final_records(invoice, pri_id):
    header = header_record_mapping(invoice)
    output_items = invoice.get("output", [])

    customer_id, crm_error = crm_customer_id(invoice)

    error_bucket = []
    if not customer_id and crm_error:
        error_bucket.append({
            "pri_id": pri_id, "type": "CRM_CUSTOMER_ERROR", "error": crm_error,
        })


    records = []

    for item in output_items:
        footer = footer_record_mapping(item)

        # Extract emp_id & emp_name
        remarks = (item.get("item_remarks2") or "").strip()
        emp_id, emp_name = "", ""

        if remarks:
            parts = [p.strip() for p in remarks.split("-")]
            emp_id = parts[0]
            emp_name = parts[1] if len(parts) > 1 else ""

        # Extract serial_no
        serial_raw = (item.get("item_remarks1") or "").strip()
        serial_nos = [s.strip() for s in serial_raw.split(",")] if serial_raw else [""]

        for serial_no in serial_nos:
            age = fetch_age_of_device(serial_no)

            records.append({
                "pri_id": pri_id,
                "emp_id": emp_id,
                "emp_name": emp_name,
                "item_remarks1__c": serial_no or "",
                "ageOfDevice": age or "",
                "customerId": customer_id or "",
                **header,
                **footer
            })

    return records, error_bucket

def push_to_bucket(invoice: Dict[str, Any], pri_id):
    records, error_bucket = build_final_records(invoice, pri_id)

    if not records:
        raise ValueError("No records generated")

    for record in records:
        mobile = record.get("customer_mobile__c", "")
        serial_no = record.get("item_remarks1__c", "")
        customer_id = record.get("customerId", "")
        invoice_id = record.get("bill_transaction_no__c", "")

        metadata = {
            "customer-id": str(customer_id),
            "invoice-id": invoice_id,
            "serial-no": str(serial_no),
            "mobile": str(mobile),
            "pri_id": str(pri_id),
        }

        mobile_pri_id = f"{mobile_prefix}/{mobile}/id/{pri_id}.json"
        store_json_data(pos_bucket, record, mobile_pri_id, metadata)

        if serial_no:
            mobile_imei = f"{mobile_prefix}/{mobile}/imei/{serial_no}.json"
            store_json_data(pos_bucket, record, mobile_imei, metadata)

            imei_key = f"{imei_prefix}/{serial_no}.json"
            store_json_data(pos_bucket, record, imei_key, metadata)

    if error_bucket:
        err_key = f"crm_error_response/{pri_id}.json"
        store_json_data(pos_bucket, error_bucket, err_key, {"pri_id": pri_id})
