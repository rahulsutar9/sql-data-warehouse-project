


use master;
go
if exists(select 1 from sys.databases where name ='Datawarehouse')
begin 
alter database Datawarehouse set single_user with rollback immediate;
drop database DataWarehouse;
end;
go

create DATABASE DataWarehouse;
go
use Datawarehouse;


create schema Bronze;
go
create schema Silver;
go
create schema Gold;
