using dexih.functions;
using dexih.transforms;
using System;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;

namespace dexih.connections.test
{
    public static class DataSets
    {
        public static int counter = 1;

        public static ReaderMemory CreateTestData()
        {
            Table table = new Table("test" + (counter++).ToString(), 0,
                new TableColumn("StringColumn", ETypeCode.String),
                new TableColumn("IntColumn", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal),
                new TableColumn("DoubleColumn", ETypeCode.Double),
                new TableColumn("BooleanColumn", ETypeCode.Boolean),
                new TableColumn("DateColumn", ETypeCode.DateTime),
                new TableColumn("DateOffsetColumn", ETypeCode.DateTimeOffset),
                new TableColumn("GuidColumn", ETypeCode.Guid),
                new TableColumn("ArrayColumn", ETypeCode.Int32, rank: 1),
                new TableColumn("MatrixColumn", ETypeCode.Int32, rank: 2)
                );

            var dateOffset = new DateTimeOffset(2015, 01, 01, 0, 0, 0, TimeSpan.FromHours(5));
            table.AddRow(new object[] { "value1", 1, 1.1m, 1.1, true, Convert.ToDateTime("2015/01/01"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value2", "2", "2.1", "2.1", "false", "2015-01-02", dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value3", 3, 3.1m, 3.1, true, Convert.ToDateTime("2015/01/03"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value4", 4, 4.1m, 4.1, false, Convert.ToDateTime("2015/01/04"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value5", 5, 5.1m, 5.1, true, Convert.ToDateTime("2015/01/05"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value6", 6, 6.1m, 6.1, false, Convert.ToDateTime("2015/01/06"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value7", 7, 7.1m, 7.1, true, Convert.ToDateTime("2015/01/07"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value8", 8, 8.1m, 8.1, false, Convert.ToDateTime("2015/01/08"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value9", 9, 9.1m, 9.1, true, Convert.ToDateTime("2015/01/09"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });
            table.AddRow(new object[] { "value10", 10, 10.1m, 10.1, false, Convert.ToDateTime("2015/01/10"), dateOffset, Guid.NewGuid(), new [] {1,1}, new [,] {{1,1},{2,2}} });

            ReaderMemory Adapter = new ReaderMemory(table);
            Adapter.Reset();
            return Adapter;
        }

        public static Table CreateTable(bool useDbAutoIncrement)
        {
            Table table = new Table("testtable" + (counter++).ToString())
            {
                Description = "The testing table"
            };
            
            table.Columns.Add(new TableColumn()
            {
                Name = "AutoIncrement",
                Description = "A key column",
                DataType = ETypeCode.Int32,
                DeltaType = useDbAutoIncrement ? EDeltaType.DbAutoIncrement : EDeltaType.AutoIncrement,
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "StringColumn",
                Description = "A string column",
                DataType = ETypeCode.String,
                DeltaType = EDeltaType.TrackingField
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "IntColumn",
                Description = "An integer column",
                DataType = ETypeCode.Int32,
                DeltaType = EDeltaType.NaturalKey
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "DecimalColumn",
                Description = "A decimal column",
                DataType = ETypeCode.Decimal,
                DeltaType = EDeltaType.TrackingField,
                Scale = 2,
                Precision = 10
            });
            
            table.Columns.Add(new TableColumn()
            {
                Name = "DoubleColumn",
                Description = "A double column",
                DataType = ETypeCode.Double,
                DeltaType = EDeltaType.TrackingField
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "BooleanColumn",
                Description = "A boolean column column",
                DataType = ETypeCode.Boolean,
                DeltaType = EDeltaType.TrackingField
            });

           
            table.Columns.Add(new TableColumn()
            {
                Name = "DateColumn",
                Description = "A date column column",
                DataType = ETypeCode.DateTime,
                DeltaType = EDeltaType.TrackingField
            });
            
            table.Columns.Add(new TableColumn()
            {
                Name = "DateOffsetColumn",
                Description = "A date offset column column",
                DataType = ETypeCode.DateTimeOffset,
                DeltaType = EDeltaType.TrackingField
            });
            
            table.Columns.Add(new TableColumn()
            {
                Name = "GuidColumn",
                Description = "A guid column",
                DataType = ETypeCode.Guid,
                DeltaType = EDeltaType.TrackingField
            });
            
            
            table.Columns.Add(new TableColumn()
            {
                Name = "ArrayColumn",
                Description = "An array column",
                DataType = ETypeCode.Int32,
                Rank = 1,
                DeltaType = EDeltaType.TrackingField
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "MatrixColumn",
                Description = "An matrix column",
                DataType = ETypeCode.Int32,
                Rank = 2,
                DeltaType = EDeltaType.TrackingField
            });
            
            return table;
        }
        
            public static Table CreateParentTable()
        {
            var table = new Table("parent", 0, 
                new TableColumn("parent_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("name", ETypeCode.String, EDeltaType.TrackingField)
            );

            return table;
        }

        public static ReaderMemory CreateParentTableData(int rows)
        {
            var table = CreateParentTable();
            
            for(var i = 0; i < rows; i++)
            {
                table.AddRow(i, $"parent {i}");
            }
            
            return new ReaderMemory(table);
        }

        public static Table CreateChildTable()
        {
            var table = new Table("child", 0, 
                new TableColumn("parent_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("child_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("name", ETypeCode.String, EDeltaType.TrackingField)
            );
            table.AddLookupIndex("parent_id");

            return table;
        }

        public static ReaderMemory CreateChildTableData(int rows)
        {
            var table = CreateChildTable();

            for(var i = 0; i < rows; i++)
            {
                table.AddRow(i, i, $"child {i}");
            }
            
            return new ReaderMemory(table);
        }

        public static Table CreateGrandChildTable()
        {
            var table = new Table("grandChild", 0, 
                new TableColumn("child_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("grandChild_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("name", ETypeCode.String, EDeltaType.TrackingField)
            );
            table.AddLookupIndex("child_id");

            return table;
        }
        
        public static ReaderMemory CreateGrandChildTableData(int rows)
        {
            var table = CreateGrandChildTable();

            for(var i = 0; i < rows; i++)
            {
                table.AddRow(i, i, $"grandChild {i}");
            }

            return new ReaderMemory(table);
        }

        // create a sample reader, containing child and grandchild arrays.
        public static Transform CreateParentChildReader(int rows)
        {
            var parent = CreateParentTableData(rows);
            var child = CreateChildTableData(rows);
            var grandChild = CreateGrandChildTableData(rows);

            var childrenColumn = new TableColumn("children", ETypeCode.Node);

            // join parent to child
            var mappings = new Mappings
            {
                new MapJoinNode(childrenColumn, child.CacheTable),
                new MapJoin(parent.CacheTable["parent_id"], child.CacheTable["parent_id"])
            };

            var parentTransform = new TransformJoin(parent, child, mappings, EJoinStrategy.Auto, EDuplicateStrategy.All, EJoinNotFoundStrategy.NullJoin, null, "Join")
            {
                Name = "Join Child",
                JoinDuplicateStrategy = EDuplicateStrategy.All
            };

            var childMappings = new Mappings
            {
                new MapJoinNode(new TableColumn("grandChildren", ETypeCode.Node), grandChild.CacheTable),
                new MapJoin(child.CacheTable["child_id"], grandChild.CacheTable["child_id"])
            };

            var childTransform = new TransformJoin()
            {
                JoinDuplicateStrategy = EDuplicateStrategy.All
            };

            var transform = childTransform.CreateNodeMapping(parentTransform, grandChild, childMappings,
                new[] {childrenColumn});

            return transform;
        }
    }
}
