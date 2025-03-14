Create or alter procedure bronze.load_bronze as
Begin
	Declare @start_time datetime, @end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
	Begin try
		set @batch_start_time = GETDATE();
		Print('===================================================')
		Print('Loading Bronze Layer')	
		Print('===================================================')

		Print('---------------------------------------------------')
		Print('Loading CRM Tables')
		Print('---------------------------------------------------')

		set @start_time = GETDATE()
		Print('>> Truncating Table: bronze.crm_cust_info')
		truncate table [bronze].[crm_cust_info]

		Print('>> Inserting Data Into: bronze.crm_cust_info')
		bulk insert [bronze].[crm_cust_info] from
		'C:\Users\bbhardwaj\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

		set @start_time = GETDATE()
		Print('>> Truncating Table: bronze.crm_prd_info')
		truncate table [bronze].[crm_prd_info]

		Print('>> Inserting Data Into: bronze.crm_prd_info')
		bulk insert [bronze].[crm_prd_info] from
		'C:\Users\bbhardwaj\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

		set @start_time = GETDATE()
		Print('>> Truncating Table: bronze.crm_sales_details')
		truncate table [bronze].[crm_sales_details]

		Print('>> Inserting Data Into: bronze.crm_sales_details')
		bulk insert [bronze].[crm_sales_details] from
		'C:\Users\bbhardwaj\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

		Print('---------------------------------------------------')
		Print('Loading ERP Tables')
		Print('---------------------------------------------------')

		set @start_time = GETDATE()
		Print('>> Truncating Table: bronze.erp_cust_az12')
		truncate table [bronze].[erp_cust_az12] 

		Print('>> Inserting Data Into: bronze.erp_cust_az12')
		bulk insert [bronze].[erp_cust_az12] from
		'C:\Users\bbhardwaj\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

		set @start_time = GETDATE()
		Print('>> Truncating Table: bronze.erp_loc_a101')
		truncate table [bronze].[erp_loc_a101]

		Print('>> Inserting Data Into: bronze.erp_loc_a101')
		bulk insert [bronze].[erp_loc_a101] from
		'C:\Users\bbhardwaj\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

		set @start_time = GETDATE()
		Print('>> Truncating Table: bronze.erp_px_cat_g1v2')
		truncate table [bronze].[erp_px_cat_g1v2]

		Print('>> Inserting Data Into: bronze.erp_px_cat_g1v2')
		bulk insert [bronze].[erp_px_cat_g1v2] from
		'C:\Users\bbhardwaj\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
		firstrow = 2,
		fieldterminator = ',',
		tablock
		)
		set @end_time = GETDATE()
		Print('>> Load Duration: ') + cast(datediff(second,@start_time,@end_time) as nvarchar) + ' seconds'
		Print('--------------')

		set @batch_end_time = GETDATE();
		Print ('===============================================')
		Print ('Loading Bronze Layer is Completed')
		Print ('- Total Duration: ') + cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) +
		' seconds';
		Print ('===============================================')	

	End try
	Begin catch
		Print ('===============================================')
		Print ('Error Occoured During Loading Bronze Layer')
		Print ('Error Message') + error_message()
		Print ('Error Message') + cast(error_number() as nvarchar)
		Print ('Error Message') + cast(error_state() as nvarchar)
		Print ('===============================================')
	End catch

End
