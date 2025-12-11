/* 
This script defines a Stored Procedure named bronze.load_bronze.

Its simple function is to automate the full loading process of all raw data from your CRM and ERP CSV files into the corresponding tables in the bronze schema.

The procedure works by first truncating (clearing) the existing data from each table and then using the powerful BULK INSERT command to quickly bring in all the new data directly from the hardcoded file paths. This ensures a complete refresh of the raw data layer every time it runs.

It also includes logging to print the time taken for each individual load and the total load, and robust error handling to catch and report any issues that might prevent the data from loading correctly.
*/

-- loading data into the tables

-- stored procedure 
create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		-- calculating the loading duration of whole bronze layer 
		set @batch_start_time = getdate();

		-- truncating and inserting the values in the table (full load)
		print '==================================';
		print 'Loading Bronze Layer';
		print '==================================';

		print'------------------------------------';
		print'Loading CRM TABLES';
		print'------------------------------------';

		-- calculating the loading duration of each tables
		set @start_time = GETDATE();
		print 'Inserting data into: bronze.crm_customer_info';
		truncate table bronze.crm_customer_info;

		bulk insert bronze.crm_customer_info
		from 'D:\SQL With Bara\dql data warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with 
		(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print ' The time taken to load the data is: ' + cast(datediff( second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '---------------------------------------------'


		set @start_time = getdate();
		print 'Inserting data into: bronze.crm_product_info';
		truncate table bronze.crm_product_info;

		bulk insert bronze.crm_product_info
		from 'D:\SQL With Bara\dql data warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print ' The time taken to load the data is: ' + cast(datediff( second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '---------------------------------------------'


		set @start_time = getdate();
		print 'Inserting data into: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		bulk insert bronze.crm_sales_Details
		from 'D:\SQL With Bara\dql data warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		print ' The time taken to load the data is: ' + cast(datediff( second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '---------------------------------------------'


		print '----------------------------------------';
		print 'Loading ERP Tables'
		print '----------------------------------------';

		set @start_time = getdate();
		print 'Inserting data into: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;

		bulk insert bronze.erp_cust_az12
		from 'D:\SQL With Bara\dql data warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		print ' The time taken to load the data is: ' + cast(datediff( second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '---------------------------------------------'


		set @start_time = getdate();
		print 'Inserting data into: bronze.erp_LOC_A101';
		truncate table bronze.erp_LOC_A101;

		bulk insert bronze.erp_LOC_A101
		from 'D:\SQL With Bara\dql data warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		print ' The time taken to load the data is: ' + cast(datediff( second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '---------------------------------------------'

		set @start_time = getdate();
		print 'Inserting data into: bronze.erp_PX_CAT_G1V2';
		truncate table bronze.erp_PX_CAT_G1V2;

		bulk insert bronze.erp_PX_CAT_G1V2
		from 'D:\SQL With Bara\dql data warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		print ' The time taken to load the data is: ' + cast(datediff( second, @start_time, @end_time) as nvarchar)+ ' seconds';
		print '---------------------------------------------'

		set @batch_end_time = getdate();
		print 'The time taken to load the bronze layer is: '+ cast(datediff( second, @start_time, @end_time) as nvarchar)+ ' seconds';
	end try
	begin catch
		print 'Error occurred while loading bronze layer'
		print 'error message: ' + error_message();
		print 'error number: ' + cast(error_number() as nvarchar);
		print 'error state: ' + cast(error_state() as nvarchar);
	end catch 
end
