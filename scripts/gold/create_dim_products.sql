create view gold.dim_products as 
select 
row_number() over(order by pro.prd_start_dt,pro.prd_key) as prodcut_key,
pro.prd_id as product_id,
pro.prd_key as prduct_name,
pro.prd_nm as product_name,
pro.cat_id as prduct_category_id,
pc.cat as product_category,
pc.subcat as product_subcategory,
pc.maintenance,
pro.prd_cost as product_cost,
pro.prd_line as product_line,
pro.prd_start_dt as start_date,
pro.prd_end_dt as end_date
from 
(
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
row_number() over (partition by prd_key order by prd_start_dt desc) as row_num
from silver.crm_prd_info pn 
) pro 
LEFT JOIN silver.erp_px_cat_g1v2 pc  
on pro.cat_id = pc.id
where pro.row_num = 1 ;
