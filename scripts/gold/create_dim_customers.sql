CREATE VIEW gold.dim_customers as 
select 
ROW_NUMBER() OVER (Order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_nmber,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
loc.cntry as country,
ci.cst_marital_status as marital_status,
ca.bdate as birth_date,
(
case when ci.cst_gndr != 'n/a' then ci.cst_gndr
else coalesce(initcap(ca.gen),'Unknown')
end
) as gender,
ci.cst_create_date as created_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca 
on ci.cst_key = ca.cid
left join silver.erp_loc_a101 loc 
on ci.cst_key = loc.cid
