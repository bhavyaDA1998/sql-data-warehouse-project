Create or alter procedure silver.load_silver as
Begin
	Declare @start_time datetime, @end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
	Begin try
		set @batch_start_time = GETDATE();
		Print('===================================================')
		Print('Loading Silver Layer')	
		Print('===================================================')

		Print('---------------------------------------------------')
		Print('Loading CRM Tables')
		Print('---------------------------------------------------')

		set @start_time = GETDATE()
Print '>> Truncating table: silver.crm_cust_info'
Truncate table silver.crm_cust_info 
print '>> Inserting Data into: silver.crm_cust_info'
insert into silver.crm_cust_info 
(cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)

Select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case when Upper(trim(cst_marital_status)) = 'S' then 'Single'
	 when Upper(trim(cst_marital_status)) = 'M' then 'Married'
else 'N/A'
end cst_marital_status,
case when Upper(trim(cst_gndr)) = 'F' then 'Female'
	 when Upper(trim(cst_gndr)) = 'M' then 'Male'
else 'N/A'
end cst_gndr,
cst_create_date
from (

Select *, ROW_NUMBER() 
over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info  where cst_id is not null
) t where flag_last = 1
set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

		set @start_time = GETDATE()
Print '>> Truncating table: silver.crm_prd_info'
Truncate table silver.crm_prd_info
print '>> Inserting Data into: silver.crm_prd_info'
Insert into silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt 
)

Select 
prd_id,

replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0) as prd_cost	,
case UPPER(trim(prd_line))
 when 'M' then 'Mountain'
 when 'R' then 'Road'
 when 'S' then 'Other Sales'
 when 'T' then 'Touring'
 else 'N/A'
 end as prd_line,
cast(prd_start_dt as Date) as prd_start_dt,
cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt) - 1 as date) as prd_end_dt
from bronze.crm_prd_info
set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

set @start_time = GETDATE()
Print '>> Truncating table: silver.crm_sales_details'
Truncate table silver.crm_sales_details
print '>> Inserting Data into: silver.crm_sales_details'
insert into silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price)

Select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or len(sls_order_dt) <> 8 then null
else cast(cast(sls_order_dt as nvarchar) as Date) 
end as sls_order_dt,
case when sls_ship_dt = 0 or len(sls_ship_dt) <> 8 then null
else cast(cast(sls_ship_dt as nvarchar) as Date) 
end as sls_ship_dt,
case when sls_due_dt = 0 or len(sls_due_dt) <> 8 then null
else cast(cast(sls_due_dt as nvarchar) as Date) 
end as sls_due_dt,
case when sls_sales is null or sls_sales <= 0  or sls_sales <> abs(sls_price) * sls_quantity then 
abs(sls_price) * sls_quantity else
 sls_sales
 end as sls_sales,
sls_quantity,
 case when sls_price is null or sls_price <= 0
 then sls_sales /  nullif(sls_quantity,0) 
 else sls_price 
 end as sls_price
from bronze.crm_sales_details
set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

set @start_time = GETDATE()
Print '>> Truncating table: silver.erp_cust_az12'
Truncate table silver.erp_cust_az12
print '>> Inserting Data into: silver.erp_cust_az12'
insert into silver.erp_cust_az12
( cid,
bdate,gen)

select 
case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid)) 
 else cid 
 end as cid,
 case when bdate > GETDATE() then null
 else bdate
 end as bdate,
case when UPPER(trim(gen)) in ('F','FEMALE') then 'Female'
	 when UPPER(trim(gen)) in ('M','MALE') then 'Male'
	 else 'N/A'
	 end as gen
from 

bronze.erp_cust_az12

set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

set @start_time = GETDATE()
Print '>> Truncating table: silver.erp_loc_a101'
Truncate table silver.erp_loc_a101 
print '>> Inserting Data into: silver.erp_loc_a101'
Insert into silver.erp_loc_a101(
cid,
cntry)

Select 
replace(cid,'-','') as cid,
case when trim(cntry) = 'DE' then 'Germany'
	 when trim(cntry) in ('US','USA') then 'United States'
	 when trim(cntry)  = '' or cntry is null then 'N/A'
	 else trim(cntry)
	 end as cntry

from bronze.erp_loc_a101

set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

set @start_time = GETDATE()
Print '>> Truncating table: silver.erp_px_cat_g1v2 '
Truncate table silver.erp_px_cat_g1v2 
print '>> Inserting Data into: silver.erp_px_cat_g1v2'
insert into silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance)

select 
id,
cat,
subcat,
maintenance

from bronze.erp_px_cat_g1v2
set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

		set @batch_end_time = GETDATE();
		Print ('===============================================')
		Print ('Loading Silver Layer is Completed')
		Print ('- Total Duration: ') + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) +
		' seconds';
		Print ('===============================================')	

	End try
	Begin catch
		Print ('===============================================')
		Print ('Error Occoured During Loading Silver Layer')
		Print ('Error Message') + error_message()
		Print ('Error Message') + cast(error_number() as nvarchar)
		Print ('Error Message') + cast(error_state() as nvarchar)
		Print ('===============================================')
	End catch

End
