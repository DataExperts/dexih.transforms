using System;
using System.Collections.Generic;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using Dexih.Utils.DataType;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class Helpers
    {        
        
        public const string BuiltInAssembly = "dexih.functions.builtIn.dll";

        public static ReaderMemory CreateSortedTestData()
        {
            var table = new Table("test", 0,
                new TableColumn("StringColumn", ETypeCode.String, EDeltaType.NaturalKey),
                new TableColumn("IntColumn", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Double, EDeltaType.NaturalKey),
                new TableColumn("DateColumn", ETypeCode.DateTime, EDeltaType.NaturalKey),
                new TableColumn("SortColumn", ETypeCode.Int32, EDeltaType.TrackingField),
                new TableColumn("ArrayColumn", ETypeCode.Int32, EDeltaType.TrackingField, 1)
            );

            table.AddRow("value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10, new [] {1,1} );
            table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9, new [] {1,1} );
            table.AddRow("value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), 8, new [] {1,1} );
            table.AddRow("value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), 7 , new [] {1,1});
            table.AddRow("value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6 , new [] {1,1});
            table.AddRow("value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5, new [] {1,1} );
            table.AddRow("value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4, new [] {1,1} );
            table.AddRow("value08", 8, 8.1, Convert.ToDateTime("2015/01/08"), 3, new [] {1,1} );
            table.AddRow("value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2, new [] {1,1} );
            table.AddRow("value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1, new [] {1,1});

            var adapter = new ReaderMemory(table, new Sorts() { new Sort("StringColumn") } );
            adapter.Reset();
            adapter.Open();
            return adapter;
        }

        public static ReaderMemory CreateUnSortedTestData()
        {
            var table = new Table("test", 0, 
                new TableColumn("StringColumn", ETypeCode.String, EDeltaType.NaturalKey),
                new TableColumn("IntColumn", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal, EDeltaType.NaturalKey),
                new TableColumn("DateColumn", ETypeCode.DateTime, EDeltaType.NaturalKey),
                new TableColumn("SortColumn", ETypeCode.Int32),
                new TableColumn("GroupColumn", ETypeCode.String)
            );

            table.AddRow("value01", 1, 1.1m, Convert.ToDateTime("2015/01/01"), 10, "Odd" );
            table.AddRow("value10", 10, 10.1m, Convert.ToDateTime("2015/01/10"), 1, "Even" );
            table.AddRow("value03", 3, 3.1m, Convert.ToDateTime("2015/01/03"), 8, "Odd" );
            table.AddRow("value02", 2, 2.1m, Convert.ToDateTime("2015/01/02"), 9, "Even" );
            table.AddRow("value08", 8, 8.1m, Convert.ToDateTime("2015/01/08"), 3, "Even" );
            table.AddRow("value04", 4, 4.1m, Convert.ToDateTime("2015/01/04"), 7, "Even" );
            table.AddRow("value07", 7, 7.1m, Convert.ToDateTime("2015/01/07"), 4, "Odd" );
            table.AddRow("value05", 5, 5.1m, Convert.ToDateTime("2015/01/05"), 6, "Odd" );
            table.AddRow("value06", 6, 6.1m, Convert.ToDateTime("2015/01/06"), 5, "Even" );
            table.AddRow("value09", 9, 9.1m, Convert.ToDateTime("2015/01/09"), 2, "Odd" );

            var adapter = new ReaderMemory(table, null);
            adapter.Reset();
            return adapter;
        }

        public static ReaderMemory CreateSortedJoinData()
        {
            var table = new Table("Join", 0);
            table.AddColumn("StringColumn", ETypeCode.String);
            table.AddColumn("IntColumn", ETypeCode.Int32);
            table.AddColumn("LookupValue", ETypeCode.String);

            table.AddRow("value01", 1, "lookup1" );
            table.AddRow("value02", 2, "lookup2" );
            table.AddRow("value03", 3, "lookup3" );
            table.AddRow("value04", 4, "lookup4" );
            table.AddRow("value05", 5, "lookup5" );
            table.AddRow("value06", 6, "lookup6" );
            table.AddRow("value07", 7, "lookup7" );
            table.AddRow("value08", 8, "lookup8" );
            table.AddRow("value09", 9, "lookup9" );

            var adapter = new ReaderMemory(table, new Sorts() { new Sort("StringColumn") });
            adapter.Reset();
            return adapter;
        }

        public static ReaderMemory CreateSortedJoinDataMissingRows()
        {
            var table = new Table("Join", 0);
            table.AddColumn("StringColumn", ETypeCode.String);
            table.AddColumn("IntColumn", ETypeCode.Int32);
            table.AddColumn("LookupValue", ETypeCode.String);

            table.AddRow("value02", 2, "lookup2" );
            table.AddRow("value03", 3, "lookup3" );
            table.AddRow("value04", 4, "lookup4" );
            table.AddRow("value06", 6, "lookup6" );
            table.AddRow("value07", 7, "lookup7" );
            table.AddRow("value08", 8, "lookup8" );
            table.AddRow("value09", 9, "lookup9" );

            var adapter = new ReaderMemory(table, new Sorts(){ new Sort("StringColumn") });
            adapter.Reset();
            return adapter;
        }

        public static ReaderMemory CreateUnSortedJoinData()
        {
            var table = new Table("Join", 0);
            table.AddColumn("StringColumn", ETypeCode.String);
            table.AddColumn("IntColumn", ETypeCode.Int32);
            table.AddColumn("LookupValue", ETypeCode.String);

            table.AddRow("value09", 9, "lookup9" );
            table.AddRow("value06", 6, "lookup6" );
            table.AddRow("value01", 1, "lookup1" );
            table.AddRow("value05", 5, "lookup5" );
            table.AddRow("value02", 2, "lookup2" );
            table.AddRow("value04", 4, "lookup4" );
            table.AddRow("value03", 3, "lookup3" );
            table.AddRow("value07", 7, "lookup7" );
            table.AddRow("value08", 8, "lookup8" );

            var adapter = new ReaderMemory(table);
            adapter.Reset();
            return adapter;
        }

        public static ReaderMemory CreateDuplicatesJoinData()
        {
            var table = new Table("Join", 0);
            table.AddColumn("StringColumn", ETypeCode.String);
            table.AddColumn("IntColumn", ETypeCode.Int32);
            table.AddColumn("LookupValue", ETypeCode.String);
            table.AddColumn("IsValid", ETypeCode.Boolean, EDeltaType.IsCurrentField);

            table.AddRow("value09", 9, "lookup9", true);
            table.AddRow("value06", 6, "lookup6", true);
            table.AddRow("value01", 1, "lookup1", true);
            table.AddRow("value05", 5, "lookup5", true);
            table.AddRow("value02", 2, "lookup2", true);
            table.AddRow("value04", 4, "lookup4a", false);
            table.AddRow("value04", 4, "lookup4", true);
            table.AddRow("value03", 3, "lookup3", true);
            table.AddRow("value07", 7, "lookup7", true);
            table.AddRow("value08", 8, "lookup8", true);

            var adapter = new ReaderMemory(table);
            adapter.Reset();
            return adapter;
        }

        public static ReaderMemory CreateValidationTestData()
        {
            var table = new Table("test", 0,
                new TableColumn("StringColumn", ETypeCode.String, EDeltaType.NaturalKey),
                new TableColumn("IntColumn", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal, EDeltaType.NaturalKey),
                new TableColumn("DateColumn", ETypeCode.DateTime, EDeltaType.NaturalKey),
                new TableColumn("SortColumn", ETypeCode.Int32, EDeltaType.TrackingField),
                new TableColumn("RejectReason", ETypeCode.String, EDeltaType.RejectedReason)
            );

            table.AddRow("value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10, "");
            table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9, "");
            table.AddRow("value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), 8, "");
            table.AddRow("value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), 7, "");
            table.AddRow("value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6, "");
            table.AddRow("value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5, "");
            table.AddRow("value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4, "");
            table.AddRow("value08", 8, 8.1, Convert.ToDateTime("2015/01/08"), 3, "");
            table.AddRow("value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2, "");
            table.AddRow("value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1, "");

            var adapter = new ReaderMemory(table, new Sorts() { new Sort("StringColumn") });
            adapter.Reset();
            return adapter;
        }

        public static Table CreateParentTable()
        {
            var table = new Table("parent", 0, 
                new TableColumn("parent_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("name", ETypeCode.String, EDeltaType.TrackingField)
            );

            return table;
        }

        public static ReaderMemory CreateParentTableData()
        {
            var table = CreateParentTable();
            
            table.AddRow(0, "parent 0");
            table.AddRow(1, "parent 1");
            table.AddRow(2, "parent 2");
            table.AddRow(3, "parent 3");
            
            return new ReaderMemory(table);
        }

        public static Table CreateChildTable()
        {
            var table = new Table("child", 0, 
                new TableColumn("parent_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("child_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("name", ETypeCode.String, EDeltaType.TrackingField)
            );

            return table;
        }

        public static ReaderMemory CreateChildTableData()
        {
            var table = CreateChildTable();
            
            table.AddRow(0, 0, "child 00");
            table.AddRow(0, 1, "child 01");
            table.AddRow(2, 20, "child 20");
            table.AddRow(3, 30, "child 30");
            
            return new ReaderMemory(table);
        }

        public static Table CreateGrandChildTable()
        {
            var table = new Table("grandChild", 0, 
                new TableColumn("child_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("grandChild_id", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("name", ETypeCode.String, EDeltaType.TrackingField)
            );

            return table;
        }
        
        public static ReaderMemory CreateGrandChildTableData()
        {
            var table = CreateGrandChildTable();
            table.AddRow(0, 0, "grandChild 000");
            table.AddRow(0, 1, "grandChild 001");
            table.AddRow(20, 200, "grandChild 200");
            table.AddRow(30, 300, "grandChild 300");
            
            return new ReaderMemory(table);
        }

        // create a sample reader, containing child and grandchild arrays.
        public static Transform CreateParentChildReader()
        {
            var parent = CreateParentTableData();
            var child = CreateChildTableData();
            var grandChild = CreateGrandChildTableData();

            var childrenColumn = new TableColumn("children", ETypeCode.Node);

            // join parent to child
            var mappings = new Mappings
            {
                new MapJoinNode(childrenColumn, child.CacheTable),
                new MapJoin(child.CacheTable["parent_id"], child.CacheTable["parent_id"])
            };

            var parentTransform = new TransformJoin(parent, child, mappings, EDuplicateStrategy.All, null, "Join")
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

        public static ReaderMemory CreateLargeTable(int rows)
        {
            var table = new Table("test");

            for (var i = 0; i < 10; i++)
                table.Columns.Add(new TableColumn("column" + i, ETypeCode.Int32, EDeltaType.NaturalKey));

            table.Columns.Add(new TableColumn("random", ETypeCode.String, EDeltaType.TrackingField) );

            for (var i = 0; i < rows; i++)
            {
                var row = new object[11];

                for (var j = 0; j < 10; j++)
                {
                    row[j] = j;
                }

                row[10] = Guid.NewGuid().ToString();

                table.AddRow(row);
            }

            return new ReaderMemory(table);
        }
    }
}
