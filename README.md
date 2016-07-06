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
[nuget-connections-sql]:     https://www.nuget.org/packages/dexih.connection.sql/
[nuget-connections-sql-img]: https://badge.fury.io/nu/dexih.connection.sql.svg
[nuget-connections-flatfile]:     https://www.nuget.org/packages/dexih.connection.flatfile/
[nuget-connections-flatfile-img]: https://badge.fury.io/nu/dexih.connection.flatfile.svg
[nuget-connections-azure]:     https://www.nuget.org/packages/dexih.connection.azure/
[nuget-connections-azure-img]: https://badge.fury.io/nu/dexih.connection.azure.svg
[nuget-connections-restful]:     https://www.nuget.org/packages/dexih.connection.restful/
[nuget-connections-restful-img]: https://badge.fury.io/nu/dexih.connection.restful.svg
[nuget-connections-soap]:     https://www.nuget.org/packages/dexih.connection.soap/
[nuget-connections-soap-img]: https://badge.fury.io/nu/dexih.connection.soap.svg
[dex-img]: http://dataexpertsgroup.com/img/dex_web_logo.png
[dex]: https://dataexpertsgroup.com

## What is this?

This is a cross platform library that provides capabilities to transform, analyze and process data.  

The key features are:
* Group, Sort, Pivot and Join data sets from heterogeneous data sources on the fly.
* Track and manage changing data and preserve change history (i.e. slowly changing dimensions).
* Leverage an extensive library of built in analytical functions, or create custom functions.
* Column level valiation and rejection rules.
* Build in data profiling and column distribution analysis.
* Optimized data connectors load from databases, flatfiles & web services.  

This library can be used as a foundation for applications the process data such as:
* Business Intelligence and reporting.
* Batch processing, Data Integration or Extract Transform Load (ETL) processing.
* Real-time analysis and alerting.

## Coming soon

In the next few weeks we will be integrating the following capabilities into the transform processing:

* Logging and resiliance.
* Additional data sources.
 

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
    TransformSource transformSource = new TransformSource(sourceReader);

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

The transformations in this library generally work best when used in conjuction with optimised SQL queries, however Sql has limits in many areas of data processin.  The transform library can provide the following benefits:

* Analytic calculations that are difficult to accomplish using SQL.  For example statitical calculations such as median, moving average are very difficult to accomplish with SQL.
* Row pivoting functions can be used to easily translate structures such as Xml and Json into tabular data.
* Join data from multiple data sources.  For example you can join  datasets on the fly from different databases, or connect to csv files, web services, or no-sql databases.
* Reduce workloads on databases can avoid database locking and performance problems when sourcing data from operational databases.
* Building reusable functions and logic that can be reapplied across multiple databases and database types.

## The Transform Class

The *transfrom* class is a feature rich implementation of the DbReader class that leverages all the benefits of the DbReader, whilst extending the functionality to allow for more sophisticated data integration.

### Caching

The *transform* class inherits one of the key benefits of using a DbReader, which is fast, forward only record streaming.  

The *transform* class enhances this by providing a built in caching mechanism.  The caching can be switched off or set to  full caching or partial caching.  When caching is enabled, this allows:
  
* Navigate backwards through previous records.
* Reset the reader to the start, without re-accessing the source database again.
* Peek at previous records (without resetting the current row).
* Search / lookup against previous loaded records.
* On demand lookup, where the cache is first referenced, and then referred to the data source if not found.


### Encryption & Hashing

Sensitive fields can easily be encrypted, decrypted or hashed through the transform control.  



## Built in Transforms

The built in transforms are implementations of the base transform class.  These can be chained together to allow virtually any type of data transformation.  

The following is a short description of the built-in transforms:

| Transform  | Description |
| ------------- | ------------- |
| Filter  | Filters the rows based on boolean conditions specified by the filter functions. |
| Group  | Allows rows to be grouped and analytic and aggregate functions to be applied to the result.  Using the "Pass Through Columns" setting, the group will retain the original number of rows and join in the analytic calculation. |
| Join  | The join allows an additional data stream to be joined to the main table. |
| Lookup  | This allows a row lookup to be performed against another data source or external function.  |
| Mapping  | Maps source fields to target fields using simple source-target mappings or advanced calculations using mapping functions.  |
| Row  | Using a row function, translates values into rows.  This can be used to pivot values, or to parse JSON/XML fields into data rows.  |
| Sort  | Sorts the dataset by one or more columns and an ascending or descending order. |
| Profile | Generates statistics a dataset, without impacting the dataset (meaning it can be inserted anywhere statistics are required).  The built in profile functions allow for detection of string patterns, data types, uniqueness et.
| Validation | The validation transform automatically checks the fitness of incoming data, against configurable validation rules, and the datatype of the data.  Where data does not pass the validation, it can be cleaned, or marked for rejection.
| Delta | Compares the incoming dataset, against another dataset, produces a delta, and generates a set of audit data.  The delta has a number of update strategies that can be configured, including update/delete detection and history preservation.  In a data warehouse this can be used to generate [type 2 slowly changing dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension).

To run a complete transformation, these transforms can be chained together in any logical sequence.  

Some tips:
* Filter early, to reduce the number of rows other transforms need to process.
* The Group, Row and the Join operate signficanlty faster with a sorted dataset.  If you can sort the data through SQL or other means, set the `SortField` property on the feeding transforms. 

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
2. The result function - Called to retrieve a result  when the grouping has completed.  This should return thee result, and specify `out` parammeters for any additional values to be returned.
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

