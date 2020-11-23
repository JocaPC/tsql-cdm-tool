/*  Script: Generate Synapse SQL Serverless view (v3.3) that reads the latest version of CDM files
*   Author: Jovan Popovic <jovanpop@microsoft.com>
*   Licence: MIT (see the end of the file)
*/

--> Prerequisite:
-- 1. Create Synapse Analytics workspace with serverless SQL pool
-- 2. Setup SAS to CDM Azure storage or add Storage Reader role for AAD user that will generate view
-- 3. Execute this script in your database.
-- 4. Create schema for the view that should be generated (or use existing schema like dbo)
-- 5. Exec cdd.run and follow the instructions

SET QUOTED_IDENTIFIER OFF;
GO

CREATE SCHEMA cdm;
GO

CREATE OR ALTER PROCEDURE cdm.run
					@model nvarchar(4000) = NULL,
					@command nvarchar(4000) = NULL,
					@entity nvarchar(100) = NULL,
					@view nvarchar(100) = NULL,
					@options nvarchar(max) = '{}'
AS BEGIN

	drop table if exists #files
	drop table if exists #groups

	declare @partitions varchar(4000) =  ISNULL(JSON_VALUE( @options, '$.partitions'), '');
	declare @defaultFileType varchar(4000) = ISNULL(JSON_VALUE( @options, '$.defaultFileType'), 'CSV'); --> file type that will be used in format setting is not in model.json
	declare @schema sysname =  ISNULL(JSON_VALUE(@options, '$.schema'), 'dbo');

	if(@command is null)
	begin
		print 'Usage:'
		print 'EXEC cdm.run '
		print '		@model = ''uri of model.json'''
		print '		@command = ''command'''
		print '		[, @options = ''{"schema":"name of schema"]}'']'
		print '		[, @entity = ''name of entity in model.json'']'
		print '		[, @view = ''name of view that will access entity'']'

		print 'Commands:'
		print '	@command = "entities"	--> List of all entities found in model.json.'
		print '	@command = "generate"	--> Generate view for the entity in model.json. @entity is required.'
		print '	@command = "script"		--> Generate CREATE VIEW script for all entities in model.json.'
		print '	@command = "files"		--> List of all files for the entitiy in model.json. @entity is required.'
		print '	@command = "columns"	--> Show columns that belong to the entity in model.json. @entity is required.'
		print '	@command = "model"		--> Show content of model.json.'

		print 'Examples:'
		print '1. Show options:'
		print 'EXEC cdm.run'

		print '2. List all entities in model.json file:'
		print 'EXEC cdm.run'
		print '	@model = N''https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json'','
		print '	@command = ''entities'''
		print ''

		print '3. Generate view for the entity ''Products'' in model.json file:'
		print 'EXEC cdm.run'
		print '	@model = N''https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json'','
		print '	@command = ''generate'', -- or ''show-view'' to just display the SQL script'
		print '	@entity = ''Product'''
		print ''

		print '4. Generate create view script for the entities in model.json file:'
		print 'EXEC cdm.run'
		print '	@model = N''https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json'','
		print '	@command = ''script'''
		print ''

		return;
	end

    if (@command not in ('entities', 'show-entities', 'generate', 'view', 'show-view', 'script', 'files', 'columns', 'model')) begin
        set @entity = @command;
        set @command = 'show-view';
    end

	if (@view is null)
		set @view = @schema + '.' + @entity;

	---------------------------------------------------------------------------
	-- Step 1/ - load model.json
	---------------------------------------------------------------------------
	declare @json nvarchar(max);
	declare @sqlGetModelJson nvarchar(max) = "
	-- TODO: Replace this with SINGLE_CLOB
	select @model_json = c.value
	from openrowset(bulk '"+@model+"',
	FORMAT='CSV',
			FIELDTERMINATOR ='0x0b', 
			FIELDQUOTE = '0x0b', 
			ROWTERMINATOR = '0x0b'
	) WITH (value varchar(max)) c;
	"

	EXECUTE sp_executesql  
		@sqlGetModelJson  
		,N'@model_json nvarchar(max) OUTPUT'  
		,@model_json = @json OUTPUT;  
	--> model.json is loaded in @modelJSON

	IF (@command IN ('show-model', 'model'))
	BEGIN
		SELECT model = JSON_QUERY( @json ) FOR JSON PATH;
		RETURN;
	END

	declare @columns nvarchar(max) = '',
			@openrowset nvarchar(max) = '',
			@sql nvarchar(max) = '',
			@mapping nvarchar(max) = '';

	declare @collation varchar(100) = (select collation_name from sys.databases where database_id = DB_ID())
	if(@collation not like '%UTF8')
		set @collation = 'Latin1_General_100_CI_AS_SC_UTF8'

	IF (@command IN ('show-entities', 'list-entities', 'entities'))
	BEGIN
		SELECT JSON_VALUE(j.value, '$.name') FROM OPENJSON (@json, '$.entities') as j
		RETURN;
	END

	IF (@command IN ('generate', 'script') AND (@entity IS NULL))
	BEGIN

    /*
EXEC cdm.run
	@model = N'https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
    @command = 'script',
    @entity = 'Customer',
    @view = 'dbo.Customer'
    */
		SELECT CONCAT(
'EXEC cdm.run
			@model = N''',@model, ''',
            @command = N''generate'',
			@entity = ''', JSON_VALUE(j.value, '$.name'), ''',
			@view = ''', @schema, '.', JSON_VALUE(j.value, '$.name'), '''')
		FROM OPENJSON (@json, '$.entities') as j

		RETURN;
	END

	-- TODO: Replace with STRING_AGG
	select 
			@columns += IIF(name IS NULL, '',  ( quotename(name) + ',')), 
			@mapping += IIF(name IS NULL, '', ( quotename(name) + ' ' + 
			 CASE dataType
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
				ELSE dataType
			 END + ','))
	from openjson(@json, '$.entities') e
	   cross apply openjson(e.value, '$.attributes')
	with (name sysname, dataType sysname)
	where json_value(e.value, '$.name') = @entity;

	SET @columns = TRIM(',' FROM @columns);
	SET @mapping = TRIM(',' FROM @mapping);
	IF (@command IN ('show-columns', 'columns') )
	BEGIN
		PRINT @columns;
		RETURN;
	END;

	with locations as (
	select location, [format], hasColumnHeader, delimiter, encoding
	from openjson(@json, '$.entities') e
	   cross apply openjson(e.value, '$.partitions')
	with (  location nvarchar(4000),
			[format] nvarchar(50) '$.fileFormatSettings."$type"',
			[hasColumnHeader] bit '$.fileFormatSettings.columnHeaders',
			delimiter nvarchar(1) '$.fileFormatSettings.delimiter',
			[encoding] varchar(10) '$.fileFormatSettings.encoding')
	where json_value(e.value, '$.name') = @entity
	)
	select *
	into #files
	from locations;

	IF (@command IN ('partitions', 'files', 'locations', 'show-partitions', 'show-files', 'show-locations') )
	BEGIN
		select * FROM #files;
		drop table if exists #files;
		drop table if exists #groups;
		RETURN;
	END

	declare @domain varchar(max);
	set @domain = (
	select top 1 SUBSTRING(f.location, 0, charindex(t.value, f.location)) + t.value
	from #files f
		cross apply string_split(f.location, '/') as t
	group by charindex(t.value, f.location), t.value, SUBSTRING(f.location, 0, charindex(t.value, f.location))
	having count(*) =  (select count(*) from #files)
	order by charindex(t.value, f.location) desc
	);

	declare @filelist varchar(max) = ''
	select @filelist += ''''+
			REPLACE(
					REPLACE(location, '.dfs.core.windows.net', '.blob.core.windows.net'),
				':443', '')
			+''','
	from #files;

	set @filelist = TRIM(' ,' FROM @filelist);

	-- Grouping by wildcards
	with t1 as (
		select  
				path = location,
				file_name = SUBSTRING(location, LEN(@domain)+1, 8000),
				[format], hasColumnHeader, delimiter, encoding
		from #files
	),
	t2 as (
	select   [format], hasColumnHeader, delimiter, encoding,
			 pattern = @domain
			  + REPLACE(REPLACE(REPLACE(TRANSLATE(file_name, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-', '#####################################'), '/#', '/*'), '#', ''), '_', '') + '*'
	from t1)
	select distinct pattern, [format], hasColumnHeader, delimiter, encoding
	into #groups
	from t2;

	with rowsets as (
	select rs = CONCAT( ' OPENROWSET( BULK ''',pattern,''',
							   FORMAT = ''', CASE [format] WHEN 'CsvFormatSettings' THEN 'CSV' ELSE @defaultFileType END ,''',
							   FIRSTROW = ',IIF(hasColumnHeader=1, '2', '1'),',
							   FIELDTERMINATOR = ''',delimiter,''')'), encoding
	from #groups
	)
	-- TODO: Replace with STRING_AGG
	SELECT  @sql += ' UNION ALL 
	SELECT ' + @columns + @partitions + '
			FROM ' + rowsets.rs + '
			WITH (' +
			case encoding
					when 'UTF-8' then REPLACE(@mapping, 'nvarchar(max)', 'varchar(max) COLLATE  ' + @collation)
					else @mapping
			end  + ') as cdm
	WHERE cdm.filepath() IN (' + @filelist +')'
	FROM rowsets;

	if( SUBSTRING( @sql, 1, 10) = ' UNION ALL')
		SET @sql = SUBSTRING( @sql, 12, 4000000);

	set @sql = 'CREATE OR ALTER VIEW ' + @view + ' AS ' + @sql;
	
	IF (@command IN ('view', 'script') )
	BEGIN
		PRINT @sql;
		drop table if exists #files
		drop table if exists #groups
		RETURN;
	END
    PRINT 'Creating view...'
	EXEC (@sql)
    PRINT 'View is created!'
	--SELECT [XML_F52E2B61-18A1-11d1-B105-00805F49916B] = @sql
	--print @sql
	
	drop table if exists #files
	drop table if exists #groups
	
END

/*
MIT License.

Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
*/


--> BUG I need to explicilty convert .dfs. to .blob.
/*
------ Instructions

EXEC cdm.run

EXEC cdm.run
	@model = N'https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'model'

EXEC cdm.run
	@model = N'https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'entities'

EXEC cdm.run
	@model = N'https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'Customer'

EXEC cdm.run
	@model = N'https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'script',
	@entity = 'Product',
	@options = '{"schema":"cdm"}'

EXEC cdm.run
	@model = N'https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'files',
	@entity = 'Product'

EXEC cdm.run
	@model = N'https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'columns',
	@entity = 'Product'

EXEC cdm.run
	@model = N'https://jovanpoptest.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'script'

*/
