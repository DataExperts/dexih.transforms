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
                new[] { new TableColumn("PurchaseOrderNumber") },  //input column
                null, null )
            },
            null
        );

        // Add daily medium and sum columns
        TransformGroup transformGroup = new TransformGroup(
            transformFilter,
            new List<ColumnPair>() //The fields to groupby
            {
            new ColumnPair(new TableColumn("OrderDate"))
            },
            new List<Function>()
            {
            StandardFunctions.GetFunctionReference("Median", new[] { new TableColumn("TotalDue") }, new TableColumn("DailyMedian"), null),
            StandardFunctions.GetFunctionReference("Sum", new[] { new TableColumn("TotalDue") }, new TableColumn("DailyTotal"), null)
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
