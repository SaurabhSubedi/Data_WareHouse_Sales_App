TRUNCATE TABLE silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    cst_create_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;


TRUNCATE TABLE silver.crm_prd_info;

INSERT INTO silver.crm_prd_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_'),
    SUBSTRING(prd_key FROM 7),
    prd_nm,
    COALESCE(prd_cost, 0),
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    prd_start_dt::date,
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::date
FROM bronze.crm_prd_info;


TRUNCATE TABLE silver.crm_sales_details;

INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
select sls_ord_num,
sls_prd_key,
sls_cust_id,
(
case 
when (sls_order_dt = 0 OR LENGTH(sls_order_dt :: text) != 8) then NULL
else cast(sls_order_dt :: text as date) 
end
) as sls_order_dt,
(
case 
when (sls_ship_dt = 0 OR LENGTH(sls_ship_dt :: text) != 8) then NULL
else cast(sls_ship_dt :: text as date) 
end
) as sls_ship_dt,
(
case 
when (sls_due_dt = 0 OR LENGTH(sls_due_dt :: text) != 8) then NULL
else cast(sls_due_dt :: text as date) 
end
) as sls_due_dt,
(
case when (sls_sales is null) or (sls_sales <=0) or (sls_sales != sls_quantity * abs(sls_price))
then sls_quantity * abs(sls_price) 
else 
sls_sales
end
) as sls_sales,
sls_quantity,
(
case when (sls_price is null) or (sls_price <=0)
then sls_sales / NULLIF(sls_quantity,0)
else 
sls_price
end
) as sls_price
from bronze.crm_sales_details;


TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
select 
(
case when cid like 'NAS%' then substring(cid,4,length(cid))
else cid
end
) as cid,
(
case when bdate > CURRENT_DATE then null 
ELSE bdate
END 
) as bdate,
(
CASE WHEN upper(trim(gen)) in ('F','FEMALE') THEN 'FEMALE'
WHEN upper(trim(gen)) in ('M','MALE') THEN 'MALE'
else 'Unknown'
end 
) as gen
from bronze.erp_cust_az12


