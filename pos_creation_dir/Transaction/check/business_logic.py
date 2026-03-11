import json
import re
import asyncio
from datetime import datetime, date, timezone
from js import console, fetch, Object 

now = datetime.now(timezone.utc)

year = now.strftime("%Y")
month = now.strftime("%m")
day = now.strftime("%d")


# --- Constants ---
INVALID_MOBILE_PLACEHOLDER = "invalid_mobile"
NON_DIGIT = re.compile(r"\D")

def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

# --- Async Cloudflare Operations ---
async def store_json_data(env, data, key, metadata=None, bucket_binding="POS_BUCKET"):
    if not key:
        raise ValueError("R2 object key cannot be empty")
    
    body = json.dumps(data, indent=2, ensure_ascii=False)
    
    # Cloudflare R2 Binding Put
    try:
        bucket = getattr(env, bucket_binding, None)
        if not bucket:
             # Fallback or error?
             # For local dev without second bucket bound in some contexts, be careful.
             # but we mandate it in wrangler.toml now.
             raise ValueError(f"Bucket binding {bucket_binding} not found in env")

        await bucket.put(key, body, custom_metadata=metadata)
        console.log(f"✅ Stored R2 ({bucket_binding}): {key}")
    except Exception as e:
        console.error(f"❌ Failed to store {key} in {bucket_binding}: {e}")
        raise

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

async def fetch_age_of_device(serial_no, env):
    # NOTE: Fetch API usage
    if not serial_no: return None
    
    # Securely access credentials and URL from environment
    security_code = getattr(env, "APX_SECURITY_CODE", "")
    user_id = getattr(env, "APX_USERID", "")
    base_url = getattr(env, "APX_API_URL", "")
    
    if not security_code or not user_id or not base_url:
        console.warn("⚠️ APX Configuration (Credentials/URL) missing in env")
        return None

    headers = {"securitycode": security_code, "userid": user_id}
    params = f"?CompanyCode=pmpl&ImeiSerialNo={serial_no}"
    
    try:
        resp = await fetch(base_url + params, method="GET", headers=headers)
        response = await resp.json()
        
        if response.get("StatusCode") != 0:
            console.warn(f"⚠️ Age API error: {response.get('StatusMessage')}")
            return None
            
        data = response.get("Data") or []
        inward_raw = data[0].get("INWARD_DATE") if data else None

        if inward_raw:
             inward_date = datetime.strptime(str(inward_raw), "%Y%m%d").date()
             return (date.today() - inward_date).days
        return None

    except Exception as e:
        console.error(f"❌ Age API error: {e}")
        return None

async def crm_customer_id(payload, env):
    # Ported logic to async fetch
    try:
        api_url = env.API_CRM_BASE_URL
        auth_token = env.API_CRM_AUTH_TOKEN
        
        if not api_url or not auth_token:
            console.warn("CRM Credentials missing in env")
            return "", None

        # ... (Transformation logic same as original) ...
        data = {
            "name": str(payload.get("customer_fname") or payload.get("customer_lname")),
            "mobile": str(payload.get("customer_mobile") or payload.get("customer_phone_no1") or payload.get("customer_phone_no2")),
            "gender": str(payload.get("customer_gender")),
            "email": str(payload.get("customer_email")),
            "type": "Customer",
            "pincode": str(payload.get("customer_pincode") or payload.get("billed_at_pincode") or payload.get("delivery_to_pincode")),
            "branch": [str(payload.get("store_code"))],
            "city": str(payload.get("customer_city") or payload.get("billed_at_city") or payload.get("delivery_to_city")),
            "state": str(payload.get("customer_state") or payload.get("billed_at_state") or payload.get("delivery_from_state")),
            "alt_mobile": str(payload.get("customer_phone_no1") or payload.get("customer_phone_no2")),
            "source": "Python",
            "status": True
        }
        
        headers = {
            "Authorization": auth_token,
            "Content-Type": "application/json"
        }
        
        resp = await fetch(api_url, method="POST", headers=headers, body=json.dumps(data))
        resp_json = await resp.json()
        
        # Logic to parse success/failure
        if resp.status in (200, 201):
             return resp_json.get("data", {}).get("customer_id", ""), None

        if resp.status == 400:
            return resp_json.get("errors", [{}])[0].get("customerId", ""), None

        return "", {"stage": "crm", "response": resp_json}

    except Exception as e:
        console.error(f"CRM Exception: {e}")
        return "", {"stage": "crm_exception", "message": str(e)}

# --- Core Logic ---
# --- Helper Functions (Ported) ---
def header_record_mapping(data):
    # Ported from Transaction/key_mapping.py
    # Removed datetime dependency in dict construction if not needed or handle it
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
        "created_At": str(datetime.now()),
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

def verify_record_mapping(data):
    records = {
        "customer_phone_no1":"",
        "item_remarks1__c":"",
        "store_code__c":"",
        "bill_status__c":"",
        "Item_Code__c":"",
        "Bill_Date__c":"",
        "bill_transaction_no__c":"",
    }
    return records

# --- Core Logic ---
async def process_record(invoice, env, pri_id):

    # ------    CRM & Records Build   -------------------------------------------------
    customer_id, crm_error = await crm_customer_id(invoice, env)

    error_bucket = []
    if not customer_id and crm_error:
        error_bucket.append({"pri_id": pri_id, "type": "CRM_CUSTOMER_ERROR", "error": crm_error,})

    # ------    Process emp_id & emp_name   -------------------------------------------
    emp_id, emp_name = "", ""
    remarks = (invoice.get("item_remarks2") or "").strip()
    if remarks:
        parts = [p.strip() for p in remarks.split("-")]
        emp_id = parts[0]
        emp_name = parts[1] if len(parts) > 1 else ""

    # ------    Final Record    -------------------------------------------------------
    header = header_record_mapping(invoice)
    footer = footer_record_mapping(invoice)
    record = {
             "pri_id": pri_id,
             "emp_id": emp_id,
             "emp_name": emp_name,
             "item_remarks1__c": "",
             "customerId": customer_id or "",
             **header,
             **footer
         }

    return record, error_bucket

async def generate_upload_tasks(record, env, pri_id):
    # 2. Upload Tasks
    # Instead of generating a list of functions, we create async tasks
    upload_tasks = []
    mobile_prefix = "mobile"
    imei_prefix = "imei"

    base_record = record if record else {}
    mobile = str(base_record.get("customer_mobile__c", ""))
    email = str(base_record.get("customer_email__c", ""))
    customer_id = base_record.get("customerId")
    invoice_id = base_record.get("bill_transaction_no__c", "")
    serial_raw = (base_record.get("item_remarks1__c") or "").strip()

    base_metadata = {
        "customer-id": str(customer_id),
        "invoice-id": invoice_id,
        "mobile": str(mobile),
        "serial-no": str(serial_raw),
        "pri_id": str(pri_id),
        "email": str(email),
    }

    # Store ID file (POS_BUCKET)
    mobile_pri_id_key = f"{mobile_prefix}/{mobile}/id/{pri_id}.json"
    upload_tasks.append(store_json_data(env, base_record, mobile_pri_id_key, metadata=base_metadata))

    # History (IMEI_BUCKET)
    history_key = f"history/{year}/{month}/{day}/{pri_id}.json"
    upload_tasks.append(
        store_json_data(env, base_record, history_key, metadata=base_metadata, bucket_binding="IMEI_BUCKET")
    )

    if customer_id:
        # (POS_BUCKET)
        cus_id_key = f"{mobile_prefix}/{mobile}/cus_id/{customer_id}.json"
        upload_tasks.append(store_json_data(env, base_record, cus_id_key, metadata=base_metadata))

    if email:
        # (POS_BUCKET)
        email_key = f"{mobile_prefix}/{mobile}/email/{email}.json"
        upload_tasks.append(store_json_data(env, base_record, email_key, metadata=base_metadata))

    serial_nos = [s.strip() for s in serial_raw.split(",") if s.strip()]

    for sn in serial_nos:
        # Await the age fetch
        age = await fetch_age_of_device(sn, env)

        imei_metadata = {
            "customer-id": str(customer_id),
            "invoice-id": invoice_id,
            "mobile": str(mobile),
            "serial-no": str(sn),
            "pri_id": str(pri_id),
            "email": str(email),
        }

        sn_record = record.copy()
        sn_record["ageOfDevice"] = age
        sn_record["item_serial_no__c"] = sn

        # 1. mobile/mobile/imei/sn (POS_BUCKET)
        key_1 = f"{mobile_prefix}/{mobile}/imei/{sn}.json"
        upload_tasks.append(store_json_data(env, sn_record, key_1, metadata=imei_metadata))

        # 2. imei/sn (IMEI_BUCKET)
        key_2 = f"{imei_prefix}/{sn}.json"
        upload_tasks.append(store_json_data(env, sn_record, key_2, metadata=imei_metadata, bucket_binding="IMEI_BUCKET"))

    # Execute all uploads
    await asyncio.gather(*upload_tasks)


    # 3. Rebuild Summary (Async)
    await rebuild_mobile_summary(env, mobile, mobile_prefix)


async def save_process_errors(env, pri_id, errors):
    if not errors or not pri_id:
        return

    # Store errors in a dedicated path as requested: crm_error response
    # crm_error_response/pri_id.json (POS_BUCKET)
    error_key = f"crm_error_response/{pri_id}.json"
    
    # Wrap in a structure with metadata if needed
    error_data = {
        "pri_id": pri_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "errors": errors
    }
    
    await store_json_data(env, error_data, error_key, bucket_binding="POS_BUCKET")


async def rebuild_mobile_summary(env, mobile):
    prefix = f"mobile/{mobile}/id/"
    
    try:
        # Async List from POS_BUCKET
        listed = await env.POS_BUCKET.list(prefix=prefix)
        keys = [obj.key for obj in listed.objects]
        
        # Read all files concurrently using helper (POS_BUCKET)
        file_contents = await asyncio.gather(*[
            read_json_data(env, k, bucket_binding="POS_BUCKET") for k in keys]
        )

        final_records = [d for d in file_contents if d]

        unique_invoices = {}
        for record in file_contents:
            if not record:
                continue

            invoice_no = record.get("Invoice_Number__c")
            if not invoice_no:
                continue  # skip records without invoice

            if invoice_no not in unique_invoices:
                unique_invoices[invoice_no] = record

        unique_records = list(unique_invoices.values())

        spends = sum(safe_float(i.get("Bill_Grant_Total__c", 0)) for i in unique_records)
        
        summary = {
            "mobile": mobile,
            "total_records": len(final_records),
            "invoice_count": len(unique_records),
            "invoice_spends": spends,
            "records": final_records
        }
        
        summary_key = f"mobile/{mobile}/mobile_summary.json"
        # Store summary in POS_BUCKET
        await store_json_data(env, summary, summary_key, metadata={"type":"summary"}, bucket_binding="POS_BUCKET")
        
    except Exception as e:
        console.error(f"Failed summary rebuild: {e}")
