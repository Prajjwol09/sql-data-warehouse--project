/*
script purpose: 
  This script is designed to reset and initialize a clean environment for a data-warehouse project.
  It begins by checking whether the DataWarehouse database already exists; if it does, the script forces the database into single-user mode to close all active connections, removes it safely, and then creates a fresh instance.
  After establishing the new database, the script sets up the foundational schemas—bronze, silver, and gold—which represent the raw, refined, and business-ready layers of a modern data-warehouse architecture. 
*/

-- drop and recreate the database 
if exists (select 1 from sys.databases where name = 'DataWarehouse')
	begin 
		alter database datawarehouse set single_user with rollback immediate;
		drop database DataWarehouse;
	end;


-- create database 'data warehouse' 
create database DataWarehouse;


-- creating schema for each layer 
create schema bronze;

create schema silver;

create schema gold;
