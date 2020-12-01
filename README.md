# T-Sql CDM tool

SQL library tool that works with Common Data Model. Currently, it works with serverless Synapse SQL pool.

> **Important**
> This is community script and CDM is not supported in serverless SQL pool. Vote for CDM support on [Azure feedback page](https://feedback.azure.com/forums/307516-azure-synapse-analytics/filters/new?category_id=387862) if you need native support. This script is provided AS-IS under [MIT licence](https://github.com/JocaPC/tsql-cdm-tool/blob/main/LICENSE).

# Setup

- Execute the [following script](https://raw.githubusercontent.com/JocaPC/tsql-cdm-tool/main/model.json/tsql-cdm-tool.sql) in your database. This script will create `cdm` schema and procedure `cdm.run`.
- Setup acess to your Azure Data Lake storage where you have placed your CDM file. As an example, you could create credential with SAS key to your storage.

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

> **Important**
> If you change the model.json file you would need to re-generate the views.

In some scenarios you might want to see the `CREAE VIEW` script before you run it and if needed modofy it. You can see the source script of the view that will be generated for the entity in model.json file using `script` command:

```sql
EXEC cdm.run
	@model = N'https://myadlsstorage.blob.core.windows.net/odipac-microsoft/ODIPAC/model.json',
	@command = 'script',
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
