from core.creds import cat_client
from fastapi import APIRouter,HTTPException
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.types import *
from pyiceberg.catalog import NoSuchNamespaceError,NamespaceAlreadyExistsError,TableAlreadyExistsError

router = APIRouter(prefix="/api/table", tags=["Tables"])


@router.get("/list/")
def get_tables(namespace):
    try:
        catalog = cat_client()
        tables = catalog.list_tables(namespace)

        if tables:
            return {"namespace": namespace, "tables": tables}
        else:
            return {"namespace": namespace, "tables": [], "message": "No tables found."}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list tables in namespace '{namespace}': {str(e)}")


@router.post("/table/create")
def create_transaction(namespace: str, table_name: str):
    table_identifier = f"{namespace}.{table_name}"

    # Step 1: Define Iceberg schema
    transaction_schema = Schema(
        NestedField(1, "pri_id", LongType(), required=True),
        NestedField(2, "tid", StringType()),
        NestedField(3, "Id", StringType()),
        NestedField(4, "OwnerId", StringType()),
        NestedField(5, "IsDeleted", DoubleType()),
        NestedField(6, "Name", StringType()),
        NestedField(7, "CreatedDate", TimestampType()),
        NestedField(8, "CreatedById", StringType()),
        NestedField(9, "LastModifiedDate", TimestampType()),
        NestedField(10, "LastModifiedById", StringType()),
        NestedField(11, "SystemModstamp", TimestampType()),
        NestedField(12, "LastActivityDate", DateType()),
        NestedField(13, "Branch_Name__c", StringType()),
        NestedField(14, "Contact__c", StringType()),
        NestedField(15, "Customer_Number__c", StringType()),
        NestedField(16, "Customer__c", StringType()),
        NestedField(17, "Invoice_Amount__c", DoubleType()),
        NestedField(18, "Invoice_Date__c", DateType()),
        NestedField(19, "Bill_Date__c", DateType()),
        NestedField(20, "customer_mobile__c", StringType(), required=True),
        NestedField(21, "customerId", StringType()),
        NestedField(22, "Customer_Last_Name__c", StringType()),
        NestedField(23, "Customer_Name__c", StringType()),
        NestedField(24, "item_cgst_perc", StringType()),
        NestedField(25, "item_sgst_perc", StringType()),
        NestedField(26, "item_sgst", StringType()),
        NestedField(27, "item_igst_perc", StringType()),
        NestedField(28, "item_cgst", StringType()),
        NestedField(29, "item_igst", StringType()),
        NestedField(30, "Deptid", StringType()),
        NestedField(31, "billed_at_branch_name", StringType()),
        NestedField(32, "billed_at_company_name", StringType()),
        NestedField(33, "billed_at_address", StringType()),
        NestedField(34, "billed_at_city", StringType()),
        NestedField(35, "billed_at_state", StringType()),
        NestedField(36, "billed_at_phone_no1", StringType()),
        NestedField(37, "billed_at_phone_no2", StringType()),
        NestedField(38, "billed_at_GSTN_no", StringType()),
        NestedField(39, "billed_at_VAT_no", StringType()),
        NestedField(40, "billed_at_state_code", StringType()),
        NestedField(41, "billed_at_PAN_no", StringType()),
        NestedField(42, "delivery_from_branch_name", StringType()),
        NestedField(43, "delivery_from_company_name", StringType()),
        NestedField(44, "delivery_from_address", StringType()),
        NestedField(45, "delivery_from_city", StringType()),
        NestedField(46, "delivery_from_state", StringType()),
        NestedField(47, "delivery_from_phone_no1", StringType()),
        NestedField(48, "delivery_from_phone_no2", StringType()),
        NestedField(49, "delivery_from_GSTN_no", StringType()),
        NestedField(50, "delivery_from_VAT_no", StringType()),
        NestedField(51, "delivery_from_state_code", StringType()),
        NestedField(52, "delivery_from_PAN_no", StringType()),
        NestedField(53, "customer_state_code", StringType()),
        NestedField(54, "Email__c", StringType()),
        NestedField(55, "IMEINumber__c", StringType()),
        NestedField(56, "Item_Brand_Name__c", StringType()),
        NestedField(57, "Item_Code__c", StringType()),
        NestedField(58, "Item_Group_Name__c", StringType()),
        NestedField(59, "Item_Name__c", StringType()),
        NestedField(60, "Item_Rate__c", DoubleType()),
        NestedField(61, "Item_Remarks__c", StringType()),
        NestedField(62, "Location__c", StringType()),
        NestedField(63, "Product__c", StringType()),
        NestedField(64, "Products__c", StringType()),
        NestedField(65, "PurchasedDate__c", DateType()),
        NestedField(66, "Service_Center__c", StringType()),
        NestedField(67, "Showroom__c", StringType()),
        NestedField(68, "Showroom_code__c", StringType()),
        NestedField(69, "Status__c", StringType()),
        NestedField(70, "bill_cancel_against__c", StringType()),
        NestedField(71, "bill_cancel_amount__c", StringType()),
        NestedField(72, "bill_cancel_date__c", StringType()),
        NestedField(73, "bill_cancel_reason__c", StringType()),
        NestedField(74, "bill_cancel_time__c", StringType()),
        NestedField(75, "bill_discount__c", StringType()),
        NestedField(76, "bill_discount_per__c", StringType()),
        NestedField(77, "bill_grand_total__c", DoubleType()),
        NestedField(78, "bill_gross_amount__c", StringType()),
        NestedField(79, "bill_modify__c", StringType()),
        NestedField(80, "bill_modify_datetime__c", TimestampType()),
        NestedField(81, "bill_modify_reason__c", StringType()),
        NestedField(82, "bill_net_amount__c", StringType()),
        NestedField(83, "bill_remarks1__c", StringType()),
        NestedField(84, "bill_remarks3__c", StringType()),
        NestedField(85, "bill_remarks4__c", StringType()),
        NestedField(86, "bill_remarks5__c", StringType()),
        NestedField(87, "bill_round_off_amount__c", DoubleType()),
        NestedField(88, "bill_service_tax__c", DoubleType()),
        NestedField(89, "bill_status__c", StringType()),
        NestedField(90, "bill_tax__c", StringType()),
        NestedField(91, "bill_tender_type__c", StringType()),
        NestedField(92, "bill_time__c", TimestampType()),
        NestedField(93, "bill_transaction_no__c", StringType()),
        NestedField(94, "bill_transaction_type__c", StringType()),
        NestedField(95, "bill_type__c", StringType()),
        NestedField(96, "customer_address__c", StringType()),
        NestedField(97, "customer_area__c", StringType()),
        NestedField(98, "customer_city__c", StringType()),
        NestedField(99, "customer_doa__c", StringType()),
        NestedField(100, "customer_dob__c", DateType()),
        NestedField(101, "customer_email__c", StringType()),
        NestedField(102, "customer_fname__c", StringType()),
        NestedField(103, "customer_gender__c", StringType()),
        NestedField(104, "customer_lname__c", StringType()),
        NestedField(105, "customer_remarks1__c", StringType()),
        NestedField(106, "customer_remarks2__c", StringType()),
        NestedField(107, "customer_remarks3__c", StringType()),
        NestedField(108, "customer_remarks4__c", StringType()),
        NestedField(109, "customer_remarks5__c", StringType()),
        NestedField(110, "customer_state__c", StringType()),
        NestedField(111, "ext_param1__c", StringType()),
        NestedField(112, "ext_param2__c", StringType()),
        NestedField(113, "ext_param3__c", StringType()),
        NestedField(114, "ext_param4__c", StringType()),
        NestedField(115, "ext_param5__c", StringType()),
        NestedField(116, "item_barcode__c", StringType()),
        NestedField(117, "item_brand_code__c", StringType()),
        NestedField(118, "item_category_code__c", StringType()),
        NestedField(119, "item_category_name__c", StringType()),
        NestedField(120, "item_color_code__c", StringType()),
        NestedField(121, "item_color_name__c", StringType()),
        NestedField(122, "item_department_code__c", StringType()),
        NestedField(123, "item_department_name__c", StringType()),
        NestedField(124, "item_discount__c", StringType()),
        NestedField(125, "item_discount_per__c", StringType()),
        NestedField(126, "item_gross_amount__c", StringType()),
        NestedField(127, "item_group__c", StringType()),
        NestedField(128, "item_net_amount__c", StringType()),
        NestedField(129, "item_quantity__c", StringType()),
        NestedField(130, "item_remarks2__c", StringType()),
        NestedField(131, "item_remarks3__c", StringType()),
        NestedField(132, "item_remarks4__c", StringType()),
        NestedField(133, "item_remarks5__c", StringType()),
        NestedField(134, "item_serial_no__c", StringType()),
        NestedField(135, "item_service_tax__c", DoubleType()),
        NestedField(136, "item_size_code__c", StringType()),
        NestedField(137, "item_size_name__c", StringType()),
        NestedField(138, "item_status__c", StringType()),
        NestedField(139, "item_sub_category_code__c", StringType()),
        NestedField(140, "item_sub_category_name__c", StringType()),
        NestedField(141, "item_tax__c", StringType()),
        NestedField(142, "store_code__c", StringType()),
        NestedField(143, "voucher_code__c", StringType()),
        NestedField(144, "voucher_type__c", StringType()),
        NestedField(145, "voucher_value__c", StringType()),
        NestedField(146, "Docid__c", StringType()),
        NestedField(147, "Customer_Address_New__c", StringType()),
        NestedField(148, "customer_mobile__c_backup", StringType()),
        NestedField(149, "billed_at_addressline2", StringType()),
        NestedField(150, "billed_at_addressline3", StringType()),
        NestedField(151, "billed_at_pincode", StringType()),
        NestedField(152, "delivery_from_branch_store_code", StringType()),
        NestedField(153, "delivery_from_addressline2", StringType()),
        NestedField(154, "delivery_from_addressline3", StringType()),
        NestedField(155, "bill_refference_no", StringType()),
        NestedField(156, "bill_refference_date", StringType()),
        NestedField(157, "bill_total_trade_deduction", StringType()),
        NestedField(158, "bill_total_trade_addition", StringType()),
        NestedField(159, "customer_addressline2", StringType()),
        NestedField(160, "customer_addressline3", StringType()),
        NestedField(161, "customer_pincode", StringType()),
        NestedField(162, "customer_GSTN_no", StringType()),
        NestedField(163, "customer_PAN_no", StringType()),
        NestedField(164, "customer_state", StringType()),
        NestedField(165, "delivery_to_Name", StringType()),
        NestedField(166, "delivery_to_addressline1", StringType()),
        NestedField(167, "delivery_to_addressline2", StringType()),
        NestedField(168, "delivery_to_addressline3", StringType()),
        NestedField(169, "delivery_to_city", StringType()),
        NestedField(170, "delivery_to_state", StringType()),
        NestedField(171, "delivery_to_pincode", StringType()),
        NestedField(172, "delivery_to_phone_no1", StringType()),
        NestedField(173, "delivery_to_phone_no2", StringType()),
        NestedField(174, "delivery_to_GSTN_no", StringType()),
        NestedField(175, "delivery_to_state_code", StringType()),
        NestedField(176, "delivery_to_PAN_no", StringType()),
        NestedField(177, "item_sno", StringType()),
        NestedField(178, "item_name", StringType()),
        NestedField(179, "item_gross_rate", StringType()),
        NestedField(180, "item_taxable_Amount", StringType()),
        NestedField(181, "item_brand_name", StringType()),
        NestedField(182, "item_product_name", StringType()),
        NestedField(183, "customer_phone_no2", StringType()),
        NestedField(184, "updated_At", TimestampType()),
        NestedField(185, "cusId_error", StringType()),
        NestedField(186, "customer_mobile__c2", StringType()),
        NestedField(187, "Bill_No__c", StringType()),
        NestedField(188, "Customer_Code_New", DoubleType()),
        NestedField(189, "bill_modify_date", StringType()),
        NestedField(190, "bill_modify_time", StringType()),
        NestedField(191, "Customer_Code__c_new", StringType()),
        NestedField(192, "Customer_Code__c", StringType()),
        NestedField(193, "bill_remarks2", StringType()),
        NestedField(194, "bill_remarks2__c", StringType()),
        NestedField(195, "item_remarks1__c", StringType()),
        NestedField(196, "Bill_Grant_Total__c", DoubleType()),
        NestedField(197, "ageOfDevice", StringType()),
    )


    # Step 2: Define partition spec
    transaction_partition_spec = PartitionSpec(
        PartitionField(
            source_id=transaction_schema.find_field("customer_mobile__c").field_id,
            field_id=2001,
            transform=IdentityTransform(),
            name="customer_mobile__c",
        ),

    )

    # Step 3: Connect to catalog
    catalog = cat_client()

    # Step 4: Ensure namespace exists
    try:
        catalog.load_namespace_properties(namespace)
    except NoSuchNamespaceError:
        catalog.create_namespace(namespace)
    except NamespaceAlreadyExistsError:
        pass

    # Step 5: Create table
    try:
        tbl = catalog.create_table(
            identifier=table_identifier,
            schema=transaction_schema,
            partition_spec=transaction_partition_spec,
            properties={
                "format-version": "2",  # <-- mandatory
                "table-type": "MERGE_ON_READ",  # <-- enable merge-on-read
                "primary-key": "pri_id",        # <-- enforce PK
                "identifier-field-ids": "1",
                "write.format.default": "parquet",
                "write.parquet.compression-codec": "zstd",
                "write.partition.path-style": "directory",
                "write.sort.order": "pri_id ASC, bill_transaction_no__c ASC",
                "write.target-file-size-bytes": "268435456"
            },
        )
        print(f"     ----> ✅ Created Iceberg table: {table_identifier}")

        # Step 6: Return confirmation
        return {
            "status": "created",
            "table": table_identifier,
            "schema_fields": [f.name for f in transaction_schema.fields],
            "partitions": [f.name for f in transaction_partition_spec.fields],
        }

    except TableAlreadyExistsError:
        return {"status": "exists", "table": table_identifier}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Table creation failed: {str(e)}")


# @router.post("/table/rename")
# def rename_table(
#     namespace: str = Query(..., description="Namespace containing the table"),
#     old_table_name: str = Query(..., description="Current table name (e.g. 'transactions')"),
#     new_table_name: str = Query(..., description="New table name (e.g. 'transactions_v2')"),
#
# ):
#
#     catalog = get_catalog_client()
#     try:
#
#         old_identifier = f"{namespace}.{old_table_name}"
#         new_identifier = f"{namespace}.{new_table_name}"
#
#         catalog.rename_table(old_identifier, new_identifier)
#
#         return {
#             "status": "success",
#             "message": f"Table renamed from '{old_table_name}' to '{new_table_name}' in namespace '{namespace}' successfully."
#         }
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to rename table '{old_table_name}' in namespace '{namespace}': {str(e)}")
#
#     finally:
#         try:
#             catalog.close()
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=f"Failed to close catalog: {str(e)}")
#
#
#
# @router.delete("/table/delete")
# def delete_table(
#     namespace: str = Query(..., description="Namespace of the table"),
#     table_name: str = Query(..., description="Name of the table to drop"),
#
# ):
#
#     catalog = get_catalog_client()
#     full_table_name = f"{namespace}.{table_name}"
#
#     try:
#         catalog.drop_table(full_table_name)
#         return {"message": f"Table '{full_table_name}' dropped successfully."}
#
#     except NoSuchTableError:
#         raise HTTPException(status_code=404, detail=f"Table '{full_table_name}' does not exist.")
#
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Failed to drop table '{full_table_name}': {str(e)}")


