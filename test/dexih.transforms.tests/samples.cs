using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

public class samples
{

    public async Task FirstTransform(SqlConnection sourceConnection, SqlConnection targetConnection)
    {
        // Retrieve the data from the database
        var cmd = new SqlCommand("select * from Sales.SalesOrderHeader ", sourceConnection);
        DbDataReader sourceReader = cmd.ExecuteReader();

        // Load the reader into transform source, which will start the transform chain.
        var transformSource = new ReaderDbDataReader(sourceReader);

        // Create a custom filter that removes records where PurchaseOrderNumber is null
        var transformFilter = new TransformFilter(
            transformSource,
            new List<TransformFunction>()
            {
            new TransformFunction(
                new Func<string, bool>((value) => value != null), //function code
                new[] { new TableColumn("PurchaseOrderNumber") },  //input column
                null, null )
            },
            null
        );

        // Add daily medium and sum columns
        var transformGroup = new TransformGroup(
            transformFilter,
            new List<ColumnPair>() //The fields to groupby
            {
            new ColumnPair(new TableColumn("OrderDate"))
            },
            new List<TransformFunction>()
            {
                Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Median").GetTransformFunction(new[] { new TableColumn("TotalDue") }, new TableColumn("DailyMedian"), null),
                Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Sum").GetTransformFunction(new[] { new TableColumn("TotalDue") }, new TableColumn("DailyTotal"), null)
            },
            true //Pass through colums = true will will pass through original fields/rows and merge in the aggregates
        );

        using (var bulkCopy = new SqlBulkCopy(targetConnection))
        {
            bulkCopy.DestinationTableName = "SalesOrderDaily";
            await bulkCopy.WriteToServerAsync(transformGroup);
        }
    }
}
