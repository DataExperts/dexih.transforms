# Data Experts Transformation Library.
### [Data Experts Group](http://dataexpertsgroup.com)

This library provides unlimited capabilities for you to transform, analyze and process data.  

The key features are:
* Will seamlessly extract and deliver data within your application logic.
* Can Group, Sort, Pivot, Join data sets from heterogeneous data sources on the fly.
* Provides an extensive library of built in analytical functions.
* Will perform any type of analytical calculations acorss your datasets.
* Is highly optimized and Fast!  Can process 100,000's of rows per second.
* Fully portable to any platform that supports the .NetStandard library.  This currently includes Windows, Mac and Linux variants.

This can be used as a foundation for applications such as:
* Business Intelligence and reporting.
* Batch processing, Data Integration or Extract Transform Load (ETL) processing.
* Real-time analysis and alerting.

## How does it work

The transformation process works by chaining `transform` objects together and then reading the end of the chain as a `DbDataReader` object.  

Here is an example that reads data from a sqlserver table, applies a filter, performs some analytics, and then writes the data back to the database.

```charp
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

Whilst SQL has it's place, there are a number of benefits leveraging a Data Integration engine versus just writing Sql queries.

* Run alalytic calculations that are difficult to accomplish using Sql.  For example median, moving average.  With this engine you leverage the richness of C# to write sophisticated and performant functions.
* Row pivoting functions can be used to easily translate structures such as Xml and Json into tabular data.
* Join data from multiple sources.  For example you can join  datasets on the fly from different databases, or connect to csv files, web services, or no-sql databases.
* Reduce workloads on databases which can avoid database locking and performance problems when sourcing data from operational databases.
* Build reusable functions and logic that can be reapplied across multiple databases and database types.

## Using Functions

Functions are used across all transforms to map, aggregate and filter data.  Functions accept values from the incoming data rows, and return values which are mapped to output columns.

There are two types of functions that can be defined; stateless or state.  

### Stateless Functions

Stateless functions are used by the mapping and filter transforms, and can only reference values stored within the current row.

The following a example creates a new function that adds two values ( i + j).  If this function is added to a mapping transform, the transform will look for the input columns `value1` and `value2` and map to the target columns `AddResult`.
```csharp
using dexih.functions;

Function function1 = 
	new Function(
		new Func<int, int, int>((i, j) => i + j), 
		new string[] { "value1", "value2" }, 
		"AddResult", 
		null
	);
	
///returns 8
Console.WriteLine (Int32)function1.RunFunction(new object[] { 6, 2 }).Value
```

Functions can also be created by referencing existing classes and method.  The following example is equivalent to the previous example, however it references a standard function.
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

* Mapping Transform
* Aggreagte Transform
* Group
