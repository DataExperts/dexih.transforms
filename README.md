# Data Experts Transformation Library.
## Data Experts Group Ltd.

The data transformation library provides unlimited capabilities for you transform, analyze and process data in virtually any way.  

The key benefits of this library are:
* Seamlessly extract and deliver data from any data sources within your application logic.
* Group, Sort, Pivot, Join any incoming data within your application from any data source.
* An extensive library of built in analytical functions.
* Perform any type of analytical calculations.
* Optimized and Fast!  Can process 100,000's of rows per second.

This can form that foundation for applications such as:
* Business Intelligence and reporting in real-time.
* Batch processing, Data Integration or Extract Transform Load (ETL) processing.
* Data processing and analytics.

## How does it work

* Uses a `DbDataReader` object and deliver the transformed data to a 'DbDataReader' object, meaning any 
* Fully portable to any platform using the .NetStandard library.

## Functions

Functions are used across the transforms to run a specific function as part of the data transformation process.  Functions accept values from the incoming data rows, and return values which are mapped to output columns.

There are two types of functions that can be defined; stateless or state.  

### Stateless Functions

Stateless functions are used by the mapping and filter transforms, and can only reference values stored within the current row.

The following a example creates a new function that adds two values ( i + j).  If this function is added to a mapping transform, the transform will look for the input columns `value1` and `value2` and map to the target columns `AddResult`.
```
Function function1 = new Function(
	new Func<int, int, int>((i, j) => i + j), 
	new string[] { "value1", "value2" }, 
	"AddResult", 
	null);
	
///returns 8
Console.WriteLine (Int32)function1.RunFunction(new object[] { 6, 2 }).Value
```

Functions can also be created by referencing existing classes and method.  The following example is equivalent to the previous example, however it references a standard function.
```
Function function2 = new Function(
	typeof(StandardFunctions), 
	"Add", 
	new string[] { "value1", "value2" }, 
	"Add", 
null);
```

* Mapping Transform
* Aggreagte Transform
* Group
