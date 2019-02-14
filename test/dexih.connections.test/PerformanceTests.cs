using dexih.connections;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using Dexih.Utils.DataType;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.transforms.Mapping;
using Xunit;
using Xunit.Abstractions;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.test
{
    public class PerformanceTests
    {
        private readonly ITestOutputHelper _output;

        public PerformanceTests(ITestOutputHelper output)
        {
            this._output = output;
        }
        
        public async Task<long> Timer(string name, Func<Task> action)
        {
            var start = Stopwatch.StartNew();
            await action.Invoke();
            var time = start.ElapsedMilliseconds;
            _output.WriteLine($"Test \"{name}\" completed in {time}ms");

            return time;
        }
        
        /// <summary>
        /// Perfromance tests should run in around 1 minute. 
        /// </summary>
        /// <param name="connection"></param>
        public async Task Performance(Connection connection, string databaseName, int rows)
        {
            await connection.CreateDatabase(databaseName, CancellationToken.None);

            //create a table that utilizes every available datatype.
            var table = new Table("LargeTable" + (DataSets.counter++));
            table.Columns.Add(new TableColumn("SurrogateKey", ETypeCode.Int64, TableColumn.EDeltaType.SurrogateKey));
            table.Columns.Add(new TableColumn("UpdateTest", ETypeCode.Int32));

            foreach (ETypeCode typeCode in Enum.GetValues(typeof(ETypeCode)))
            {
                if (typeCode == ETypeCode.Binary && !connection.CanUseBinary) continue;
                if (typeCode == ETypeCode.CharArray && !connection.CanUseCharArray) continue;
                if (typeCode == ETypeCode.Enum || typeCode == ETypeCode.Object || typeCode == ETypeCode.Unknown || typeCode == ETypeCode.Char || typeCode == ETypeCode.Node) continue;

                table.Columns.Add(new TableColumn()
                {
                    Name = "column" + typeCode,
                    DataType = typeCode,
                    MaxLength = 50,
                    DeltaType = TableColumn.EDeltaType.TrackingField,
                    AllowDbNull = true
                });
            }

            //create the table
            await connection.CreateTable(table, true, CancellationToken.None);

            //add rows using the min/max values for each of the datatypes.
            var buffer = 0;
            for (long i = 0; i < rows; i++)
            {
                var row = new object[table.Columns.Count];

                row[0] = i; //surrogate key column
                row[1] = 0;

                //load the rows with min and max values.
                for (var j = 2; j < table.Columns.Count; j++)
                {
                    // alternate rows with min values, max values and null values.
                    switch (i % 3)
                    {
                        case 0:
                            row[j] = connection.GetConnectionMaxValue(table.Columns[j].DataType, table.Columns[j].MaxLength.Value);
                            break;
                        case 1:
                            row[j] = connection.GetConnectionMinValue(table.Columns[j].DataType, table.Columns[j].MaxLength.Value);
                            break;
                        case 2:
                            row[j] = null;
                            break;
                    }
                        
                }
                table.Data.Add(row);
            }

            var readerMemory = new ReaderMemory(table);
            var cleanedReader = new ReaderConvertDataTypes(connection, readerMemory);

            var time = await Timer($"Reader with data converter applied", async () =>
            {
                var i = 0;
                while (await cleanedReader.ReadAsync()) i++;
                Assert.Equal(rows, i);
            });

            _output.WriteLine($"Reader with data converter, columns: {table.Columns.Count}, row average {1000*rows/time} r/s, column average: {(1000*rows*table.Columns.Count)/time}");

            //start a datawriter and insert the test data
            await connection.DataWriterStart(table);
                    
            readerMemory = new ReaderMemory(table);
            cleanedReader = new ReaderConvertDataTypes(connection, readerMemory); 
                    
            time = await Timer($"Run bulk insert for {rows} rows.", async () =>
            {
                await connection.ExecuteInsertBulk(table, cleanedReader, CancellationToken.None);    
            });
            _output.WriteLine($"Run bulk insert for {rows} rows. Columns: {table.Columns.Count}, row average {1000*rows/time} r/s, column average: {(1000*rows*table.Columns.Count)/time}");
            
            table.Data.Clear();


            //count rows using reader
            var count = 0;
            var reader = connection.GetTransformReader(table);
            var sortQuery = new SelectQuery()
            {
                Sorts = new List<Sort>() {new Sort("SurrogateKey")},
                Table = table.Name
            };
            
            await reader.Open(0, sortQuery, CancellationToken.None);

            while (await reader.ReadAsync())
            {
                if (count == 0)
                {
                    for (var j = 2; j < table.Columns.Count; j++)
                    {
                        Assert.Equal(connection.GetConnectionMaxValue(table.Columns[j].DataType, table.Columns[j].MaxLength.Value), reader[j]);
                    }
                    
                }
                if (count == 1)
                {
                    for (var j = 2; j < table.Columns.Count; j++)
                    {
                        if (reader[j] == null)
                        {
                            Assert.Equal(connection.GetConnectionMinValue(table.Columns[j].DataType, table.Columns[j].MaxLength.Value), "");
                        }
                        else
                        {
                            Assert.Equal(connection.GetConnectionMinValue(table.Columns[j].DataType, table.Columns[j].MaxLength.Value), reader[j]);    
                        }
                        
                    }
                }

                if (count == 2)
                {
                    for (var j = 2; j < table.Columns.Count; j++)
                    {
                        Assert.Null(reader[j]);
                    }
                }

                count++;
            }
            
            Assert.Equal(rows, count);


            var updateQueries = new List<UpdateQuery>();

            //run a update on 10% of rows
            for (long i = 0; i < rows / 10; i++)
            {
                var updateColumns = new List<QueryColumn>();

                //use this column to validate the update success
                var updateColumn = new QueryColumn(table.Columns[1], 1);
                updateColumns.Add(updateColumn);

                //load the columns with random values.
                for (var j = 2; j < table.Columns.Count; j++)
                {
                    updateColumn = new QueryColumn(table.Columns[j],
                        connection.GetConnectionMaxValue(table.Columns[j].DataType));
                    updateColumns.Add(updateColumn);
                }
                updateQueries.Add(new UpdateQuery()
                {
                    Filters = new List<Filter>() {new Filter("SurrogateKey", Filter.ECompare.IsEqual, i)},
                    Table = table.Name,
                    UpdateColumns = updateColumns
                });
            }

            await Timer($"Run update in 10% of rows for {rows} rows.", async () =>
            {
                await connection.ExecuteUpdate(table, updateQueries, CancellationToken.None);    
            });
            

            //check the table loaded 1,000 rows updated successully
            var selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() {new SelectColumn("UpdateTest")},
                Filters = new List<Filter>()
                {
                    new Filter(new TableColumn("UpdateTest", ETypeCode.Int32), Filter.ECompare.IsEqual, 1)
                },
                Rows = -1,
                Table = table.Name
            };

            //count rows using reader
            count = 0;
            reader = connection.GetTransformReader(table);
            await reader.Open(0, selectQuery, CancellationToken.None);
            while (await reader.ReadAsync()) count++;
            Assert.True(count == rows / 10, "row count = " + count);

            var deleteQueries = new List<DeleteQuery>
            {
                //delete 10,000 rows
                new DeleteQuery()
                {
                    Filters = new List<Filter>()
                    {
                        new Filter(new TableColumn("UpdateTest", ETypeCode.Int32), Filter.ECompare.IsEqual, 1)
                    },
                    Table = table.Name,
                }
            };

            await Timer($"Delete 10% of rows for {rows} rows.",
                async () =>
                {
                    await connection.ExecuteDelete(table, deleteQueries, CancellationToken.None); 
                    
                });

            selectQuery = new SelectQuery()
            {
                Columns = new List<SelectColumn>() {new SelectColumn("SurrogateKey")},
                //                Filters = new List<Filter>() { new Filter() { Column1 = "column1", CompareDataType = DataType.ETypeCode.String, Operator = Filter.ECompare.NotEqual, Value2 = "updated" } },
                Rows = -1,
                Table = table.Name
            };

            //count rows using reader
            count = 0;
            reader = connection.GetTransformReader(table);
            await reader.Open(0, null, CancellationToken.None);
            while (await reader.ReadAsync()) count++;
            Assert.True(count == rows - rows / 10, "row count = " + count);


            //run a preview
            var query = table.DefaultSelectQuery(50);
            var previewTable = await connection.GetPreview(table, query, CancellationToken.None);
            Assert.NotNull(previewTable);
            Assert.Equal(50, previewTable.Data.Count);
        }


        /// <summary>
        /// Perfromance tests should run in around 1 minute. 
        /// </summary>
        /// <param name="connection"></param>
        public async Task PerformanceTransformWriter(Connection connection, string databaseName, long rows)
        {
            await connection.CreateDatabase(databaseName, CancellationToken.None);

            //create a table that utilizes every available datatype.
            var table = new Table("LargeTable" + (DataSets.counter++));

            table.Columns.Add(
                new TableColumn("SurrogateKey", ETypeCode.Int32, TableColumn.EDeltaType.SurrogateKey)
                {
                    IsIncrementalUpdate = true
                });
            table.Columns.Add(new TableColumn("UpdateTest", ETypeCode.Int32));

            foreach (ETypeCode typeCode in Enum.GetValues(typeof(ETypeCode)))
            {
                if (typeCode == ETypeCode.Binary && connection.CanUseBinary) continue;
                if(typeCode != ETypeCode.Binary) continue;
                
                table.Columns.Add(new TableColumn()
                {
                    Name = "column" + typeCode,
                    DataType = typeCode,
                    MaxLength = 50,
                    DeltaType = TableColumn.EDeltaType.TrackingField
                });
            }

            //create the table
            await connection.CreateTable(table, true, CancellationToken.None);

            //add rows.
            var buffer = 0;
            for (var i = 0; i < rows; i++)
            {
                var row = new object[table.Columns.Count];

                row[0] = i;
                row[1] = 0;

                //load the rows with random values.
                for (var j = 2; j < table.Columns.Count; j++)
                {
                    var dataType = DataType.GetType(table.Columns[j].DataType);
                    if (i % 2 == 0)
                        row[j] = connection.GetConnectionMaxValue(table.Columns[j].DataType, 20);
                    else
                        row[j] = connection.GetConnectionMinValue(table.Columns[j].DataType);
                }
                table.Data.Add(row);
                buffer++;

                if (buffer >= 5000 || rows == i + 1)
                {
                    //start a datawriter and insert the test data
                    await connection.DataWriterStart(table);
                    await connection.ExecuteInsertBulk(table, new ReaderMemory(table), CancellationToken.None);

                    table.Data.Clear();
                    buffer = 0;
                }
            }

            var targetTable = table.Copy();
            targetTable.AddAuditColumns();
            targetTable.Name = "TargetTable";
            await connection.CreateTable(targetTable, false, CancellationToken.None);

            var targetTransform = connection.GetTransformReader(targetTable);

            //count rows using reader
            var transform = connection.GetTransformReader(table);
            var mappings = new Mappings(true);
            transform = new TransformMapping(transform, mappings);
            transform = new TransformValidation(transform, null, false);
            transform = new TransformDelta(transform, targetTransform, TransformDelta.EUpdateStrategy.Reload, 1, false);
            await transform.Open(0, null, CancellationToken.None);

            var writer = new TransformWriter();
            var writerResult = new TransformWriterResult();
            await connection.InitializeAudit(writerResult, 0, 0, "Datalink", 1, 2, "Test", 1, "Source", 2, "Target", TransformWriterResult.ETriggerMethod.Manual, "Test", CancellationToken.None);
            Assert.NotNull(writerResult);

            var result = await writer.WriteAllRecords(writerResult, transform, targetTable, connection, null, null, null, null, CancellationToken.None);

            Assert.Equal(rows, writerResult.RowsCreated);

            //check the audit table loaded correctly.
            var auditTable = await connection.GetTransformWriterResults(0, 1, null, "Datalink", writerResult.AuditKey, null, true,
                false, false, null, 1, 2, false, CancellationToken.None);
            
            Assert.Equal(writerResult.RowsCreated, auditTable[0].RowsCreated);
            Assert.Equal(rows + 1, Convert.ToInt64(auditTable[0].MaxIncrementalValue));
        }
    }
}