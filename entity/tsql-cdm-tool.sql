/*  Script: Generate Synapse SQL Serverless view that reads the latest version of CDM files
*   Author: Jovan Popovic <jovanpop@microsoft.com>
*   Licence: MIT (see the end of the file)
*/

--> Prerequisite: setup SAS to CDM storage or add Storage Reader role for AAD user that will generate view
--> Create schema for the view that should be generated (or use existing schema like dbo)

SET QUOTED_IDENTIFIER OFF
GO

IF(NOT EXISTS ( SELECT * FROM sys.schemas WHERE name = 'cdm'))
    EXEC sp_executesql N'CREATE SCHEMA cdm';
GO

CREATE OR ALTER PROCEDURE cdm.load (@path VARCHAR(4000), @json NVARCHAR(MAX) OUT)
AS BEGIN

	declare @sqlGetModelJson nvarchar(max) = "
	-- TODO: Replace this with SINGLE_CLOB
	select @model_json = c.value
	from openrowset(bulk '"+@path+"',
			FORMAT='CSV',
			FIELDTERMINATOR ='0x0b', 
			FIELDQUOTE = '0x0b', 
			ROWTERMINATOR = '0x0b'
	) WITH (value varchar(max)) c;";

	EXECUTE sp_executesql  
		@sqlGetModelJson  
		,N'@model_json nvarchar(max) OUTPUT'  
		,@model_json = @json OUTPUT;  
	--> model.json is loaded in @json

END
GO

CREATE OR ALTER FUNCTION cdm.files (@json nvarchar(max), @dataSource sysname = 'CDM')
RETURNS TABLE
AS RETURN (
    with files as ( 
    select  rootLocation, regularExpression = ISNULL(regularExpression,'/*.'+substring(traitReference, 21, 30)),
            format = substring(traitReference, 21, 30),
            FIRSTROW = iif(FIRSTROW = 'true', '2', '1'), 
            FIELDTERMINATOR
    from openjson (@json, '$.entities')
        with (entityName  sysname, entityPath nvarchar(4000), dataPartitionPatterns nvarchar(max) AS JSON) e
        cross apply openjson(e.dataPartitionPatterns)
                    with (rootLocation nvarchar(4000), regularExpression nvarchar(4000), 
                            traitReference nvarchar(4000) '$.exhibitsTraits[0].traitReference',
                            arguments nvarchar(max) '$.exhibitsTraits[0].arguments' AS JSON)
                        cross apply ( 
                        select FIRSTROW = MAX(FIRSTROW), FIELDTERMINATOR = MAX(FIELDTERMINATOR)
                        from (
                                select  FIRSTROW = (select value where name = 'columnHeaders'),
                                        FIELDTERMINATOR = (select value where name = 'delimiter')
                                from openjson(arguments) with (name sysname, value varchar(50)) 
                        ) as a
                        ) as b
    )
    select sql = CONCAT("SELECT * FROM OPENROWSET(BULK '", rootLocation, regularExpression, "', DATA_SOURCE = '", @dataSource, "', FORMAT = '", format, "', FIELDTERMINATOR = '", FIELDTERMINATOR, "', FIRSTROW = ", FIRSTROW, ' ) as ', format)
    from files
)
GO

CREATE FUNCTION cdm.columns (@json nvarchar(max))
RETURNS TABLE
AS RETURN (
    select  name, dataType = CASE dataFormat
            WHEN 'int64' THEN 'bigint'
            WHEN 'int32' THEN 'int'
            WHEN 'dateTime' THEN 'datetime2'
            WHEN 'datetimeoffset' THEN 'datetimeoffset'
            WHEN 'decimal' THEN 'decimal'
            WHEN 'double' THEN 'float'
            WHEN 'boolean' THEN 'varchar(5)' --> True or False
            WHEN 'string' THEN 'nvarchar(max)'
            WHEN 'guid' THEN 'uniqueidentifier'
            WHEN 'json' THEN 'nvarchar(max)'
            ELSE dataFormat
         END 
    from openjson (@json, '$.definitions[0].hasAttributes')
        with (name  sysname, dataFormat nvarchar(30))
)
