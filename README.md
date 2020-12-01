# tsql-cdm-tool
SQL library tool that works with Common Data Model. Currently it works with Serverless Synapse SQL pool.

> Important
> This is community script and CDM is not supported in serverless SQL pool. Vote for CDM support on Azure feedback page if you need native support.

# Setup


# View the content of model.json file

Procedure `cdm.run` enables you to read the content of CDM model.json file:
```sql
EXEC cdm.run
	@model = N'https://myadlsstorage.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json'
```

You can also get a list of entities from the model.json file:
```sql
EXEC cdm.run
	@model = N'https://myadlsstorage.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'entities'
```

# Creating views on top of entities

If you want to access your CDM data stored in Azure Data Lake storage, you need provide URI fo model.json and secify the name of entity as command:
```sql
EXEC cdm.run
	@model = N'https://myadlsstorage.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'Customer'
```

This action will create a view on top of you CDM data. Now, you are able to query CDM data using the following view:

```sql
SELECT TOP 10 * FROM dbo.Customer
```

> Important
> If you change the model.json file you would need to re-generate the views.

In some scenarios you might want to see the `CREAE VIEW` script before you run it and if needed modofy it. You can see the source script of the view that will be generated for the entity in model.json file using `script` command:

```sql
EXEC cdm.run
	@model = N'https://myadlsstorage.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'script', -- or 'generate' to create the view
	@entity = 'Product',
	@options = '{"schema":"cdm"}'
```


This tool cannot automatically create the views for all entities in model.json file. However, it enables you to generate T-SQL script that will create the views for all entities in model.json file: 
```sql
EXEC cdm.run
	@model = N'https://myadlsstorage.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'script'
```

You can get one or even all scripts that are returned with this command and execute them to create the views.

# More info

Run the `cdm.run   procedure without parameters to see other options:
```sql
EXEC cdm.run
```
