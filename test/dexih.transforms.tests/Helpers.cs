using System;
using System.Collections.Generic;
using dexih.functions;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms.tests
{
    public class Helpers
    {
        public static ReaderMemory CreateSortedTestData()
        {
            var table = new Table("test", 0,
                new TableColumn("StringColumn", ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("IntColumn", ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DateColumn", ETypeCode.DateTime, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("SortColumn", ETypeCode.Int32, TableColumn.EDeltaType.TrackingField)
            );

            table.AddRow("value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10 );
            table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9 );
            table.AddRow("value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), 8 );
            table.AddRow("value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), 7 );
            table.AddRow("value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6 );
            table.AddRow("value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5 );
            table.AddRow("value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4 );
            table.AddRow("value08", 8, 8.1, Convert.ToDateTime("2015/01/08"), 3 );
            table.AddRow("value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2 );
            table.AddRow("value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1);

            var adapter = new ReaderMemory(table, new List<Sort> { new Sort("StringColumn") } );
            adapter.Reset();
            return adapter;
        }

        public static ReaderMemory CreateUnSortedTestData()
        {
            var table = new Table("test", 0, 
                new TableColumn("StringColumn", ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("IntColumn", ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DateColumn", ETypeCode.DateTime, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("SortColumn", ETypeCode.Int32),
                new TableColumn("GroupColumn", ETypeCode.String)
            );

            table.AddRow("value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10, "Odd" );
            table.AddRow("value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1, "Even" );
            table.AddRow("value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), 8, "Odd" );
            table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9, "Even" );
            table.AddRow("value08", 8, 8.1, Convert.ToDateTime("2015/01/08"), 3, "Even" );
            table.AddRow("value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), 7, "Even" );
            table.AddRow("value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4, "Odd" );
            table.AddRow("value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6, "Odd" );
            table.AddRow("value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5, "Even" );
            table.AddRow("value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2, "Odd" );

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

            var adapter = new ReaderMemory(table, new List<Sort> { new Sort("StringColumn") });
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

            var adapter = new ReaderMemory(table, new List<Sort> { new Sort("StringColumn") });
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
            table.AddColumn("IsValid", ETypeCode.Boolean, TableColumn.EDeltaType.IsCurrentField);

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
                new TableColumn("StringColumn", ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("IntColumn", ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DateColumn", ETypeCode.DateTime, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("SortColumn", ETypeCode.Int32, TableColumn.EDeltaType.TrackingField),
                new TableColumn("RejectReason", ETypeCode.String, TableColumn.EDeltaType.RejectedReason)
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

            var adapter = new ReaderMemory(table, new List<Sort> { new Sort("StringColumn") });
            adapter.Reset();
            return adapter;
        }



        public static ReaderMemory CreateLargeTable(int rows)
        {
            object[] row;
            var table = new Table("test");

            for (var i = 0; i < 10; i++)
                table.Columns.Add(new TableColumn("column" + i, ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey));

            table.Columns.Add(new TableColumn("random", ETypeCode.String, TableColumn.EDeltaType.TrackingField) );

            for (var i = 0; i < rows; i++)
            {
                row = new object[11];

                for (var j = 0; j < 10; j++)
                {
                    row[j] = j;
                }

                row[10] = Guid.NewGuid().ToString();

                table.Data.Add(row);
            }

            return new ReaderMemory(table);
        }
    }
}
