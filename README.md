# Data Transformation Libraries
[![][dex-img]][dex]

[![][build-img]][build]

## Nuget Packages

|Package|Nuget Link|
|---|---|
|Transforms - (Start here) The base transforms library, table caching, and a basic connection.|[![][nuget-transforms-img]][nuget-transforms]|
|Functions - Standard functions, data type conversations and other support libraries for the transforms.|[![][nuget-functions-img]][nuget-functions]|
|Connections Sql - Connections for Sql Server and Sqlite.|[![][nuget-connections-sql-img]][nuget-connections-sql]|
|Connections FlatFile - Connections for various types of delimited flat files.|[![][nuget-connections-flatfile-img]][nuget-connections-flatfile]|
|Connections Azure - Connections Azure Table Storage and Azure File Storage.|[![][nuget-connections-azure-img]][nuget-connections-azure]|
|Connections Restful - Connection to source data form Rest based web services.|[![][nuget-connections-restful-img]][nuget-connections-restful]|
|Connections Soap - Connection to source data form Soap based web services.|[![][nuget-connections-soap-img]][nuget-connections-soap]|
---

[build]:     https://ci.appveyor.com/project/dataexperts/dexih-transforms
[build-img]: https://ci.appveyor.com/api/projects/status/q5n1npq7r5a4udle?svg=true
[nuget-transforms]:     https://www.nuget.org/packages/dexih.transforms/
[nuget-transforms-img]: https://badge.fury.io/nu/dexih.transforms.svg
[nuget-functions]:     https://www.nuget.org/packages/dexih.functions/
[nuget-functions-img]: https://badge.fury.io/nu/dexih.functions.svg
[nuget-connections-sql]:     https://www.nuget.org/packages/dexih.connections.sql/
[nuget-connections-sql-img]: https://badge.fury.io/nu/dexih.connections.sql.svg
[nuget-connections-flatfile]:     https://www.nuget.org/packages/dexih.connections.flatfile/
[nuget-connections-flatfile-img]: https://badge.fury.io/nu/dexih.connections.flatfile.svg
[nuget-connections-azure]:     https://www.nuget.org/packages/dexih.connections.azure/
[nuget-connections-azure-img]: https://badge.fury.io/nu/dexih.connections.azure.svg
[nuget-connections-restful]:     https://www.nuget.org/packages/dexih.connections.webservice.restful/
[nuget-connections-restful-img]: https://badge.fury.io/nu/dexih.connections.webservice.restful.svg
[nuget-connections-soap]:     https://www.nuget.org/packages/dexih.connections.webservice.soap/
[nuget-connections-soap-img]: https://badge.fury.io/nu/dexih.connections.webservice.soap.svg
[dex-img]: http://dataexpertsgroup.com/img/dex_web_logo.png
[dex]: https://dataexpertsgroup.com

## What is this?

This is a cross-platform library that provides capabilities to transform, analyze and process data.  

The key features are:
* Group, Sort, Pivot and Join data sets from heterogeneous data sources on the fly.
* Track and manage changing data and preserve change history (i.e. slowly changing dimensions).
* Leverage an extensive library of built-in analytical functions, or create custom functions.
* Column level validation and rejection rules.
* Build in data profiling and column distribution analysis.
* Optimized data connectors load from databases, flat files & web services.  

This library can be used as a foundation for applications the process data such as:
* Business Intelligence and reporting.
* Batch processing, Data Integration or Extract Transform Load (ETL) processing.
* Real-time analysis and alerting.

## Coming soon

In the next few weeks we will be integrating the following capabilities into the transform processing:

* Logging and resilience.
* Additional data sources.
* More documentation!

## How does it work?

The transformation engine works by chaining `transform` objects together and then reading data from the  end of the chain as a `DbDataReader` object.  

Here is an example that reads data from a sqlserver table, applies a filter, performs some analytics, and then writes the data to a sql server table.

```csharp
public void FirstTransform(SqlConnection sourceConnection, SqlConnection targetConnection)
{
    // Retrieve the data from the database
    SqlCommand cmd = new SqlCommand("select * from Sales.SalesOrderHeader ", sourceConnection);
    DbDataReader sourceReader = cmd.ExecuteReader();

    // Load the reader into transform source, which will start the transform chain.
    ReaderDbDataReader transformSource = new ReaderDbDataReader(sourceReader);

    // Create a custom filter that removes records where PurchaseOrderNumber is null
    TransformFilter transformFilter = new TransformFilter(
        transformSource,
        new List<Function>()
        {
            new Function(
                new Func<string, bool>((value) => value != null), //function code
                new[] { "PurchaseOrderNumber" },  //input column
                null, null )
        }
    );

    // Add median, and sum calculation
    TransformGroup transformGroup = new TransformGroup(
        transformFilter,
        new List<ColumnPair>() //The fields to groupby
        {
            new ColumnPair("OrderDate")
        },
        new List<Function>()
        {
            StandardFunctions.GetFunctionReference("Median", new[] { "TotalDue" }, "DailyMedian", null),
            StandardFunctions.GetFunctionReference("Sum", new[] { "TotalDue" }, "DailyTotal", null)
        },
        true //Pass through colums = true will will pass through original fields/rows and merge in the aggregates
    );

    using (var bulkCopy = new SqlBulkCopy(targetConnection))
    {
        bulkCopy.DestinationTableName = "SalesOrderDaily";
        bulkCopy.WriteToServer(transformGroup);
    }
}
```
## Why not just use SQL?

The transformations in this library generally work best when used in conjunction with optimised SQL queries, however Sql has limits in many areas of data processing.  The transform library can provide the following benefits:

* Analytic calculations that are difficult to carry out using SQL.  For example statistical calculations such as median, moving average are very difficult to accomplish with SQL.
* Row pivoting functions can be used to easily translate structures such as Xml and Json into tabular data.
* Join data from multiple data sources.  For example you can join  datasets on the fly from different databases, or connect to csv files, web services, or no-sql databases.
* Reduce workloads on databases can avoid database locking and performance problems when sourcing data from operational databases.
* Building reusable functions and logic that can be applied across multiple databases and database types.

## The Transform Class

The *transform* class is a feature rich implementation of the DbDataReader class that leverages all the benefits of the DbDataReader, whilst extending the functionality to allow for more advanced data integration.

### Basics

The transform class inherits the `DbDataReader` class and can be used in the same way to read data.

The following is a simple example of reading records from a `Transform` object.

```csharp
while(await transform.ReadAsync())
{
    for(int i =0; i < transform.FieldCount; i++)
    {
        Console.WriteLine("Field: {0}, Value: {1}", transform.GetName(i), transform[i].ToString());
    }
}
```

### Caching

The *transform* class enhances the `DbDataReader` class by providing a built-in caching mechanism.  The caching can be switched off or set to  full caching or partial caching.  When caching is enabled, this allows:
  
* Navigate backwards through previous records.
* Reset the reader to the start, without re-accessing the source database again.
* Peek at previous records (without resetting the current row).
* Search / lookup against previous loaded records.
* On demand lookup, where the cache is first referenced, and then referred to the data source if not found.

#### Caching Settings

The transform can store a cache of records already read.  By default the cache is off.  There are two types of caching options.  
* *OnDemandCache* - Will retain records that have been read by the `ReadAsync()` or `LookupRow()` function.  Whenever the `LookupRow` function is called the transform will first check the cache for the row, and if not found ask the underlying conneciton for the row.
* *PreLoadCache* - Will load all records into memory the first time the `LookupRow` function is called.  It will then only refer to the cache when retrieving additional records.

The transform can also set a maximum cache size, which causes the transform to store the last *n* rows.  This can be useful to manage memory usage on large tables, and store rows when a commit fails.

The following example sets the caching to OnDemandCache and stores a maximum of 1000 rows.
```csharp
transform.SetCacheMethod(ECacheMethod.OnDemandCache, 1000);
```

#### Navigating through the cache

The following methods are available to navigate through cached records:
* *SetRowNumber(int rowNumber)* - Sets a specific row number.  When the `ReadAsync()` is called, it will start reading from this number.  An exception will be raised if the row number exceeds the number of cached rows.
* *RowPeek(int rowNumber, object[] values)* - Populates the `values` array with the rows values as the specified `rowNumber`.

#### Lookup Function

The lookup function can be used to retrieve values from the cache or directly through a supported connection.

The syntax for the lookup function is:
`Task<ReturnValue<object[]>> LookupRow(List<Filter> filters)`

The lookup function applies the following logic to retrive a record:
1. Looks in the cache for the record.
2. Executes a *direct lookup* if supported by the `Connection`.
3. Scans through each row until the lookup is found.

The following connections support direct lookups:
* ConnectionSqlite
* ConnectionSql
* ConnectionAzure
* ConnectionWebServiceRestful
* ConnectionWebServiceSoap
* ConnectionMemory

The following example shows how to use the lookup function

```csharp
//gets the transform reader.
var reader = connection.GetTransformReader(table, null);

//open the reader
openResult = await reader.Open();
if(!openResult.Success)
    throw new Exception("Open Reader failed:" + openResult.Message);

//set the caching
reader.SetCacheMethod(ECacheMethod.PreLoadCache);

//set a filter for the lookup
var filters = new List<Filter> { new Filter("IntColumn", Filter.ECompare.IsEqual, 5) };

//call the lookup
var returnLookup = await reader.LookupRow(filters);

if(returnLookup.Success)
    Console.WriteLine("The record was found.");
else
    Console.WriteLine("The record was not found.");

```

*Note:  Lookups best used where there are a low number of lookup values, or when calling a function (such as a web service) where the lookup has specific parameters (such as current datetime).  Where there are two (or more) large datasets that need to be joined together, the JoinTransform will generally perform faster.*


### Chaining Transforms

Transforms can be chained together to create a complete data transformation.  

A transform chain should use to the following pattern:

1.  Start with a `reader`
2.  Chain together the general transforms as required to produce the neccessary data transformation.
3.  End the chain with an optional `validation transform`, followed by a `delta transform`
4.  The end point can either be read directly as a DbReader would be read (i.e. iterating through the `ReadAsync()` function) or the `TransformWriter` class can be used to write the result set to a connection.

#### Creating a Reader

A reader is the start point of a data transformation chain.  The following method can be used to create a reader.

***Memory Reader***

A memory reader uses a `table` object populated with data.  The following sample creates a memory reader:

```csharp
//Create the table class
Table table = new Table("test", 0,
    new TableColumn("StringColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
    new TableColumn("IntColumn", DataType.ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey),
    new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal, TableColumn.EDeltaType.NaturalKey),
    new TableColumn("DateColumn", DataType.ETypeCode.DateTime, TableColumn.EDeltaType.NaturalKey),
    new TableColumn("SortColumn", DataType.ETypeCode.Int32, TableColumn.EDeltaType.TrackingField)
            );

//Populate the table with some data
    table.AddRow("value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10 );
    table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9 );
    table.AddRow("value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), 8 );
    table.AddRow("value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), 7 );
    table.AddRow("value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6 );

//Initialize the ReaderMemory, with an indicator that the "StringColumn" is sorted
ReaderMemory reader = new ReaderMemory(table, new List<Sort>() { new Sort("StringColumn") } );

```

***Row Creator Reader***

A row generator reader, is a simple reader that generates a sequence of numbers.  This can be used to generate test data, or dimensions such as a date dimension that requires a fixed sequence of values.The following sample creates a row generator reader:

```csharp
//Create a sequence of rows from 1, 1000 incrementing by 2
ReaderRowCreator rowCreator = new ReaderRowCreator(1, 1000, 2);
```

***Using a standard DbDataReader***

Most standard database connections in .Net can be used to generate a DbDataReader object.  Any standard DbDataReader can be used as a data source for a transformation chain by using the `ReaderDbDataReader` class.

The following example uses a Sql Server table to start a transformation chain:

```csharp
// Retrieve the data from the database
using(var sqlConnection = new SqlConnection("Data Source=(localDb)\11.0;Trusted_Connection=True;Initial Catalog=AdventureWorks2012"))
using(SqlCommand sqlCommand = new SqlCommand("select * from Sales.SalesOrderHeader ", sqlConnection))
using(DbDataReader sqlReader = sqlCommand.ExecuteReader())
{
// Load the Dbreader into the ReaderDbDataReader which will start the transform chain.
ReaderDbDataReader reader = new ReaderDbDataReader(sqlReader);

}
```

***Using the built in dexih.connections***

The dexih.connections can be used to generate a reader from various type of sources, dependent on the connection being used.  See the documentation for each individual connection for specifics.  

The following example uses the `ConnectionSqlite` to read from a local sqlite database table.

```csharp
//Create a connection to sqlite
var connection = new ConnectionSqlite()
{
    Name = "Test Connection",
    NtAuthentication = true,
    ServerName = "c:\\data\database.sqlite"
};

//get the table structure.
var tableResult = connection.GetSourceTableInfo("SalesTable");
if(!table.Success)
    throw new Exception("Could not retrieve table information");
var table = tableResult.Value;

//create a new connection to the database.
var newConnection = connection.NewConnection();

var readerResult = connection.GetDatabaseReader(table, newConnection);

if(!readerResult.Success)
   throw new Exception("Reader issue: " + readerResult.Message);

//new instance of the reader
var reader = readerResult.Value;

```

###Creating the Transforms

After creating a `reader` transform to start the transformation, the next step is to chain the transforms together to perform the desired data processing.

The following example assumes a `reader` has already been created, and then chains a sort and mapping transform together.

```csharp
//create use the sort transform to sort by the "StringField"
var sortField = new List<Sort> { new Sort("StringField", Descending} )};
var sortTransform = new TransformSort(reader, sortField);

//maps the "StringColumn" field to the name "OutputColumn"
var mappingFields = List<ColumnPair> mappingFields = new List<ColumnPair>() { new ColumnPair("StringColumn" "OutputColumn")};
var mappingTransform = new TransformMapping(sortTransform, mappingFields, null);

//read the data using mappingTransform.ReadAsync();
```

###Delivering the data###

Saving or publishing data can be done using the functions that support DbDataReader (such as the SqlBulk) or using the `TransformWriter` class.

The following is a simple example that uses the SqlBulkCopy class to write a transform result to a Sql Server table.

```csharp
using (var bulkCopy = new SqlBulkCopy(targetConnection))
{
    bulkCopy.DestinationTableName = "SalesOrderDaily";
    bulkCopy.WriteToServer(mappingTransform);
}
```

The library also includes a `TransformWriter` class.  This class can be used to save data to any of the available `Connection` libraries.  It also includes logic to apply operations such as update, delete, preserve, and reject which are produced by the TransformValidation and TransformDelta controls.

The following code shows how to use this class:
```csharp
TransformWriter writer = new TransformWriter();
TransformWriterResult writerResult = new TransformWriterResult();
var returnResult = await writer.WriteAllRecords(writerResult, transform, targetTable, targetConnection, rejectTable, rejectConnection, CancellationToken.None);

if(!returnResult.Success)
    throw new Exception("The writer failed to run with message: " returnResult.Message);

if(!writerResult.RunStatus == Abended)
    throw new Exception("The writer failed with message: " writerResult.Message);

Console.WriteLine("Finished with status {0}, and processed {1} rows.", writerResult.RunStatus, writerResult.RowsTotal.ToString());
```

### Table Class

The table class is required by every transform, and it provides metadata and caching capabilities.

A table can either be created manually, or by calling the `GetSourceTableInfo` function in a connection class.

To retrieve from a connection:

```csharp
var getTableResult = connection.GetSourceTableInfo("MyTable", null);
if(!getTableResult.Success)
    throw new Exception("There was an issue getting the table: " + getTableResult.Message);

Table table = getTableResult.Value;

```


### Encryption & Hashing

Sensitive fields can easily be encrypted, decrypted or hashed through the transform control.  


## Built in Transforms

The built-in transforms are implementations of the base transform class.  These can be chained together to allow almost any type of data transformation.  

The following is a short description of the built-in transforms:

| Transform  | Description |
| ------------- | ------------- |
| Filter  | Filters the rows based on boolean conditions specified by the filter functions. |
| Group  | Allows rows to be grouped and analytic and aggregate functions to be applied to the result.  Using the "Pass Through Columns" setting, the group will keep the original number of rows and join in the analytic calculation. |
| Join  | The join allows an extra input data stream to be joined to the main table. |
| Lookup  | This allows a row lookup to be performed against another data source or external function.  |
| Mapping  | Maps source fields to target fields using simple source-target mappings or advanced calculations using mapping functions.  |
| Row  | Using a row function, translates values into rows.  This can be used to pivot values, or to parse JSON/XML fields into data rows.  |
| Sort  | Sorts the dataset by one or more columns and an ascending or descending order. |
| Profile | Generates statistics a dataset, without impacting the dataset (meaning it can be inserted anywhere statistics are required).  The built in profile functions allow for detection of string patterns, data types, uniqueness et.
| Validation | The validation transform automatically checks the fitness of incoming data, against configurable validation rules, and the data type of the data.  Where data does not pass the validation, it can be cleaned, or marked for rejection.
| Delta | Compares the incoming dataset, against another dataset, produces a delta, and generates a set of audit data.  The delta has a number of update strategies that can be configured, including update/delete detection and history preservation.  In a data warehouse this can be used to generate [type 2 slowly changing dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension).

To run a complete transformation, these transforms can be chained together in any logical sequence.  

Some tips:
* Filter early, to reduce the number of rows other transforms need to process.
* The Group, Row and the Join run faster with a sorted dataset.  If you can sort the data through SQL or other means, set the `SortField` property on the feeding transforms. 

## Using Functions

Functions are used across all transforms to map, aggregate and filter data.  Functions accept values from the incoming data rows, and return values which are mapped to output columns.

There are two types of functions that can be defined; stateless or state.

### Stateless Functions

Stateless functions are used by the mapping and filter transforms, and do not maintain state between rows.

The following example shows how to create a new function that adds two values ( i + j).  If this function is added to a mapping transform, the transform will look for the input columns `value1` and `value2` and map to the target column `AddResult`.  Additional output columns can be defined by specifying fields in the output

```csharp
using dexih.functions;

Function function1 = 
	new Function(
		new Func<int, int, int>((i, j) => i + j), 
		new string[] { "value1", "value2" }, 
		"AddResult", 
		null //specify more output fields here.
	);
	
///test the function writes value - 8
Console.WriteLine (Int32)function1.RunFunction(new object[] { 6, 2 }).Value
```

Functions can also be created by referencing an existing class and method.  The following example is equivalent to the previous example, however it references a standard function.
```csharp
using dexih.functions;

Function function2 = 
	new Function(
		typeof(StandardFunctions), 
		"Add", 
		new string[] { "value1", "value2" }, 
		"AddResult", 
		null
	);
```
### State functions

Functions containing a state are used for aggregations, analytics, and row pivoting where multiple rows of data are required to run the function.  To implement a state function, three discrete functions must be defined:

1. The primary function - Called for each row in the grouping.  This should be `void` function with the input parameters specifying the column values to be processed.
2. The result function - Called to retrieve a result  when the grouping has completed.  This should return thee result, and specify `out` parameters for any additional values to be returned.
3. The reset function - this is called to reset variables and start receiving data for thee next group.

Here is a simple example that implements the sum function:

```csharp
class CalculateSum
{
	int total = 0;
	
	public void Sum(int value)
	{
		total = total + value;
	}
	
	public int SumResult()
	{
		return total;
	}
	
	public void SumReset()
	{
		total =  0;
	}
}
```

##Using the Transforms

###Filter Transform

Filters the rows based on boolean conditions specified by the filter functions.

The filter transform consists of one or more functions that return a boolean result.  A record will filtered through when any of the functions are false.

The following example uses the Equal function to remove any rows where the JunkColumn is equal to the value "junk".

```csharp
//set a Conditions list
List<Function> Conditions = new List<Function>();

//use the built in IsEqual function.
Function Function = StandardFunctions.GetFunctionReference("IsEqual");
Function.Inputs = new dexih.functions.Parameter[] {
        new dexih.functions.Parameter("JunkColumn", ETypeCode.String, true, null, "StringColumn" ),
        new dexih.functions.Parameter("Compare", ETypeCode.String, false, "junk") };

//use the NotCondition property to change the funciton to `not equal`.
Function.NotCondition = true

//add the function to the conditions list
Conditions.Add(Function);

//create the new filter transform with the conditions applied.
TransformFilter TransformFilter = new TransformFilter(InTransform, Conditions);
```

###Sort Transform

Sorts the dataset by one or more columns and an ascending or descending order.

The following exmaple sorts the incoming data by Column1 (ascending) and Column2 (descending).

```csharp

var SortFields = new List<Sort> { 
    new Sort("Column1", Sort.EDirection.Ascending) 
    new Sort("Column2", Sort.EDirection.Descending) 
};
TransformSort TransformSort = new TransformSort(Source, SortFields);    
```

**Note:**
The sort transform outputs the `OutputSortFields` property.  This property is used to inform downstream transforms that the dataset is sorted.  This is used by the Group, Row, Join and Delta transforms to indicate the data is sorted as needed by this.  If the inbound transform already has `OutputSortFields` set to the same sort order as the sort transform, the sort will do nothing.  

###Mapping Transform

Maps source fields to target fields using simple source-target mappings or advanced calculations using mapping functions.

The following example uses the ColumnPair class to perform some column mappings, and a function to perform a substring.

```csharp
List<ColumnPair> MappingColumns = new List<ColumnPair>();

//map the sourceFieldName to the targetFieldName
MappingColumns.Add(new ColumnPair("sourceFieldName", "targetFieldName"));

//map some other fields without any name change.
MappingColumns.Add(new ColumnPair("createDate"));
MappingColumns.Add(new ColumnPair("updateDate"));

List<Function> MappingFunctions = new List<Function>();

//use the substring function to limit the 'bigField' to 20 characters
Function = StandardFunctions.GetFunctionReference("Substring");
Function.TargetColumn = "trimmedField";
Function.Inputs = new dexih.functions.Parameter[] {
        new dexih.functions.Parameter("name", ETypeCode.String, true, null, "bigField" ),
        new dexih.functions.Parameter("start", ETypeCode.Int32, false, 0),
        new dexih.functions.Parameter("length", ETypeCode.Int32, false, 20) 
};
MappingFunctions.Add(Function);

//create the mapping transform
transformMapping = new TransformMapping(InTransform, MappingColumns, MappingFunctions);
```

The mapping transform can also use the property `PassThroughColumns`.  If this is set to `true`, the mapping transform will map any source fields that haven't already been used by a mapping function or mapping columnpair.

###Join Transform

The join allows an extra input data stream to be joined to the primary table.



###Lookup Transform

This allows a row lookup to be performed against another data source or external function.


###Group Transform

Allows rows to be grouped and analytic and aggregate functions to be applied to the result.  Using the "Pass Through Columns" setting, the group will keep the original number of rows and join in the analytic calculation. 

###Row Transform

Using a row function, translates values into rows.  This can be used to pivot values, or to parse JSON/XML fields into data rows.


###Profile Transform

Generates statistics a dataset, without impacting the dataset (meaning it can be inserted anywhere statistics are required).  The built in profile functions allow for detection of string patterns, data types, uniqueness et.

###Validation Transform 

The validation transform automatically checks the fitness of incoming data, against configurable validation rules, and the data type of the data.  Where data does not pass the validation, it can be cleaned, or marked for rejection.
| Delta | Compares the incoming dataset, against another dataset, produces a delta, and generates a set of audit data.  The delta has a number of update strategies that can be configured, including update/delete detection and history preservation.  In a data warehouse this can be used to generate [type 2 slowly changing dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension).

