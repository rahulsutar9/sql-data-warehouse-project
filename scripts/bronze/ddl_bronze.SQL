/*
========================================================
DDL Script : Create Bronze Table

Script Purpose : 
This Script creates tables in the 'bronze' schema ,dropping existing tables if they already exists.
Run this script to redefine the DDL structure of 'bronze' tables.

=======================================================
*/
  
if object_id ('bronze.crm_cust_info', 'U') is not null
drop table bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
	cst_id int ,
	[cst_key] nvarchar(50) ,
	[cst_firstname] nvarchar (50)	,
	[cst_lastname] nvarchar (50) ,
	[cst_marital_status] nvarchar (50) ,
	[cst_gndr] nvarchar (50) ,
	[cst_create_date] date 
) ;

if OBJECT_ID('bronze.crm_prd_info','U') is not null
drop table bronze.crm_prd_info ;
CREATE TABLE Bronze.crm_prd_info(
	prd_id int ,
	prd_key nvarchar(50) ,
	prd_nm nvarchar(50),
	prd_cost int ,
	prd_line nvarchar(10) ,
	prd_start_dt date ,
	prd_end_dt date);
	Select * from Bronze.crm_prd_info

if object_id ('bronze.crm_sales_details', 'U') is not null
drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num NVARCHAR (50),
sls_prd_key NVARCHAR (50),
sls_cust_key INT,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int);


if object_id ('bronze.erp_cust_az12', 'U') is not null
drop table bronze.erp_cust_az12;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50));

if object_id ('bronze.erp_loc_a101', 'U') is not null
drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50));


if object_id ('bronze.erp_px_cat_g1v2', 'U') is not null
drop table bronze.erp_px_cat_g1v2 ;
create table bronze.erp_px_cat_g1v2 (
id nvarchar (50),
cat nvarchar(50),
subcat nvarchar(50),
maintanance nvarchar(50));

select * from bronze.crm_cust_info
select count(*) from Bronze.crm_cust_info


	------------------------------------------------------------------------
	-------------------------------BULK INSERTION--------------------------
	-----------------------------------------------------------------------
	truncate table bronze.crm_cust_info
		bulk insert bronze.crm_cust_info
		from 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
		firstrow = 2,
		fieldterminator=',',
		tablock
			);
		--select * from Bronze.crm_cust_info;

		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info 
		FROM 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH ( 
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
		--select * from Bronze.crm_prd_info

		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details 
		FROM 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK);
		--select * from Bronze.crm_sales_details

	

		truncate table bronze.erp_cust_az12
		bulk insert bronze.erp_cust_az12
		from 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
		firstrow = 2,
		fieldterminator =',',
		tablock )
		--select * from Bronze.erp_cust_az12;

		truncate table bronze.erp_loc_a101
		bulk insert bronze.erp_loc_a101
		from 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
		firstrow = 2,
		fieldterminator =',',
		tablock );
		--select * from Bronze.erp_loc_a101



		truncate table bronze.erp_px_cat_g1v2
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\SQL\Baraa\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
		firstrow = 2,
		fieldterminator =',',
		tablock );
		--select * from Bronze.erp_px_cat_g1v2

