


	------------------------------------------------------------------------
	-------------------------------BULK INSERTION--------------------------
	-----------------------------------------------------------------------

	Create or ALter Procedure bronze.load_procedure as
	BEGIN
	DECLARE @START_TIME DATETIME,@END_TIME DATETIME,@BATCH_START_TIME DATETIME,@BATCH_END_TIME DATETIME;
	SET @BATCH_START_TIME=GETDATE();
	begin try
	PRINT'============================================';
	print'            LOADING BRONZE LAYER';
	print'============================================';

	print'--------------------------------------------';
	print'             LOADING CRM TABLES';
	print'--------------------------------------------';

	SET @START_TIME =GETDATE();

	print '>>TRUNCATING TABLE :bronze.crm_cust_info ';
	truncate table bronze.crm_cust_info;
	print'>> INSERTING DATA INTO: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator=',',
		tablock
			);
	--select * from Bronze.crm_cust_info;
	SET @END_TIME =GETDATE();
	PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(MILLISECOND,@START_TIME,@END_TIME) AS NVARCHAR) +'MILLISECOND';
	print''
	print''

	SET @START_TIME =GETDATE();
	print '>>TRUNCATING TABLE :bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;
	print'>> INSERTING DATA INTO: bronze.crm_prd_info';

		BULK INSERT bronze.crm_prd_info 
		FROM 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH ( 
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
		--select * from Bronze.crm_prd_info
	SET @END_TIME =GETDATE();
	PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(MILLISECOND,@START_TIME,@END_TIME) AS NVARCHAR) +'MILLISECOND';

	print''
	print''

	SET @START_TIME =GETDATE();
		print '>>TRUNCATING TABLE :bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;

		print'>> INSERTING DATA INTO: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details 
		FROM 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
		SET @END_TIME =GETDATE();
	PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(MILLISECOND,@START_TIME,@END_TIME) AS NVARCHAR) +'MILLISECOND';

		--select * from Bronze.crm_sales_details

	print'--------------------------------------------'
	print'             LOADING ERP TABLES'
	print'--------------------------------------------'

	SET @START_TIME =GETDATE();
	print '>>TRUNCATING TABLE :bronze.erp_cust_az12 ';
		truncate table bronze.erp_cust_az12
	print'>> INSERTING DATA INTO: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
		firstrow = 2,
		fieldterminator =',',
		tablock )
	SET @END_TIME =GETDATE();
	PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(MILLISECOND,@START_TIME,@END_TIME) AS NVARCHAR) +'MILLISECOND';

		--select * from Bronze.erp_cust_az12;

		SET @START_TIME =GETDATE();
		print''
		print''
		print '>>TRUNCATING TABLE :bronze.erp_loc_a101 ';
		truncate table bronze.erp_loc_a101
		print'>> INSERTING DATA INTO: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
		firstrow = 2,
		fieldterminator =',',
		tablock );
	SET @END_TIME =GETDATE();
	PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(MILLISECOND,@START_TIME,@END_TIME) AS NVARCHAR) +'MILLISECOND';

		--select * from Bronze.erp_loc_a101


		print''
		print''
		
		SET @START_TIME =GETDATE();
		print '>>TRUNCATING TABLE :bronze.erp_px_cat_g1v2 ';
		truncate table bronze.erp_px_cat_g1v2
		print'>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
		firstrow = 2,
		fieldterminator =',',
		tablock );
	SET @END_TIME =GETDATE();
	SET @BATCH_END_TIME =GETDATE();
	PRINT'>> LOAD DURATION: ' + CAST(DATEDIFF(MILLISECOND,@START_TIME,@END_TIME) AS NVARCHAR) +'MILLISECOND';
	PRINT '>>LOAD DURATION FOR BATCH:' + CAST(DATEDIFF(MILLISECOND,@BATCH_START_TIME,@BATCH_END_TIME) AS NVARCHAR) + 'MILLISECOND';
		--select * from Bronze.erp_px_cat_g1v2
		END try
	begin catch
PRINT '================================================================='
PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER'
PRINT 'ERROR MESSAGE' + ERROR_MESSAGE ();
PRINT'ERROR MESSAGE'   + CAST(ERROR_NUMBER () AS NVARCHAR);
PRINT'ERROR MESSAGE'   + CAST(ERROR_STATE () AS NVARCHAR);
PRINT '================================================================='
	end catch
	end 
