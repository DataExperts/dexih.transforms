//using System;
//using System.Collections.Generic;
//using System.Data.Common;
//using System.Data.SqlClient;
//using System.Threading.Tasks;
//using dexih.functions;
//using dexih.functions.Mappings;
//using dexih.functions.Parameter;
//using dexih.transforms;
//
//public class Samples
//{
//
//    public async Task FirstTransform(SqlConnection sourceConnection, SqlConnection targetConnection)
//    {
//        // Retrieve the data from the database
//        var cmd = new SqlCommand("select * from Sales.SalesOrderHeader ", sourceConnection);
//        DbDataReader sourceReader = cmd.ExecuteReader();
//
//        // Load the reader into transform source, which will start the transform chain.
//        var transformSource = new ReaderDbDataReader(sourceReader);
//
//        var parameters = new Parameters
//        {
//            Inputs = new List<Parameter>() {new ParameterColumn("value", new TableColumn("PurchaseOrderNumber"))}
//        };
//        var function = new TransformFunction(new Func<string, bool>(value => value != null), null);
//        var mappings = new Mappings
//        {
//            new MapFunction(function, parameters)
//        };
//
//        var transformFilter = new TransformFilter(transformSource, mappings);
//
//
//        mappings = new Mappings(true)
//        {
//            new MapFunction(Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Median").GetTransformFunction(null), )
//        };
//        // Add daily medium and sum columns
//        var transformGroup = new TransformGroup(
//            transformFilter,
//            new List<ColumnPair> //The fields to groupby
//            {
//            new ColumnPair(new TableColumn("OrderDate"))
//            },
//            new List<TransformFunction>
//            {
//                Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Median").GetTransformFunction(new[] { new TableColumn("TotalDue") }, new TableColumn("DailyMedian"), null, new GlobalVariables(null)),
//                Functions.GetFunction("dexih.functions.BuiltIn.AggregateFunctions", "Sum").GetTransformFunction(new[] { new TableColumn("TotalDue") }, new TableColumn("DailyTotal"), null, new GlobalVariables(null))
//            },
//            null,
//            true //Pass through colums = true will will pass through original fields/rows and merge in the aggregates
//        );
//
//        using (var bulkCopy = new SqlBulkCopy(targetConnection))
//        {
//            bulkCopy.DestinationTableName = "SalesOrderDaily";
//            await bulkCopy.WriteToServerAsync(transformGroup);
//        }
//    }
//}
