# Data Experts Transformation Library.
### [Data Experts Group](http://dataexpertsgroup.com)

## Status

[![Build Status](https://ci.appveyor.com/api/projects/status/q5n1npq7r5a4udle?svg=true)](https://ci.appveyor.com/project/dataexperts/dexih-transforms)


## What is this?

This library provides unlimited capabilities for you to transform, analyze and process data.  

The key features are:
* Seamlessly extract and deliver data within your application logic.
* Easily Group, Sort, Pivot and Join data sets from heterogeneous data sources on the fly.
* An extensive library of built in analytical functions.
* Perform any type of analytical calculations across your datasets.
* Runs fast!  Can easily process 100,000's of rows per second.
* Uses standard database classes and can be integrated with all popular databases.
* Fully portable to any platform that supports the .NetStandard library (currently includes Windows, Mac and Linux variants).

This powerful library can be used as a foundation for applications such as:
* Business Intelligence and reporting.
* Batch processing, Data Integration or Extract Transform Load (ETL) processing.
* Real-time analysis and alerting.

## Comming soon

In the next few weeks we will be integrating the following capabilities into the transform processing:

* Data profiling.
* Manage change data capture
* Preserve change history (i.e. slowly changing dimensions)
* Column level valiation and rejection rules.
 

## How does it work?

The transformation process works by chaining `transform` objects together and then reading the end of the chain as a `DbDataReader` object.  

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

    // Add daily medium and sum columns
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

The transformations in this library generally work best when used in conjuction with optimised SQL queries, however SQL has it's limits.  Some of benefits leveraging the Data Transformation engine versus writing SQL queries:

* Analytic calculations that are difficult to accomplish using SQL.  For example statitical calculations such as median, moving average are very difficult to accomplish with SQL.
* Row pivoting functions can be used to easily translate structures such as Xml and Json into tabular data.
* Join data from multiple sources.  For example you can join  datasets on the fly from different databases, or connect to csv files, web services, or no-sql databases.
* Reduce workloads on databases can avoid database locking and performance problems when sourcing data from operational databases.
* Building reusable functions and logic that can be reapplied across multiple databases and database types.

## Using Transforms

The following transforms are provided in this library.

| Transform  | Description |
| ------------- | ------------- |
| Filter  | Filters the rows based on boolean conditions specified by the filter functions. |
| Group  | Allows rows to be grouped and analytic and aggregate functions to be applied to the result.  Using the "Pass Through Columns" setting, the group will retain the original number of rows and join in the analytic calculation. |
| Join  | The join allows an additional data stream to be joined to the main table. |
| Lookup  | This allows a row lookup to be performed against another data source or external function.  |
| Mapping  | Maps source fields to target fields using simple source-target mappings or advanced calculations using mapping functions.  |
| Row  | Using a row function, translates values into rows.  This can be used to pivot values, or to parse JSON/XML fields into data rows.  |
| Sort  | Sorts the dataset. |

To run a complete transformation, these transforms should be chained together.  

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

