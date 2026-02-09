CREATE OR REPLACE VIEW gold.dim_products
 AS
 SELECT row_number() OVER (ORDER BY pro.prd_start_dt, pro.prd_key) AS product_key,
    pro.prd_id AS product_id,
    pro.prd_key AS product_number,
    pro.prd_nm AS product_name,
    pro.cat_id AS product_category_id,
    pc.cat AS product_category,
    pc.subcat AS product_subcategory,
    pc.maintenance,
    pro.prd_cost AS product_cost,
    pro.prd_line AS product_line,
    pro.prd_start_dt AS start_date,
    pro.prd_end_dt AS end_date
   FROM ( SELECT pn.prd_id,
            pn.cat_id,
            pn.prd_key,
            pn.prd_nm,
            pn.prd_cost,
            pn.prd_line,
            pn.prd_start_dt,
            pn.prd_end_dt,
            row_number() OVER (PARTITION BY pn.prd_key ORDER BY pn.prd_start_dt DESC) AS row_num
           FROM silver.crm_prd_info pn) pro
     LEFT JOIN silver.erp_px_cat_g1v2 pc ON pro.cat_id::text = pc.id::text
  WHERE pro.row_num = 1;

ALTER TABLE gold.dim_products
    OWNER TO postgres;
