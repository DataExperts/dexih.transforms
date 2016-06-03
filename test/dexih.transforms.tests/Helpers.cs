using dexih.functions;
using System;
using System.Collections.Generic;

namespace dexih.transforms.tests
{
    public class Helpers
    {
        public static SourceTable CreateSortedTestData()
        {
            Table table = new Table("test", new List<TableColumn>() {
                new TableColumn("StringColumn", DataType.ETypeCode.String),
                new TableColumn("IntColumn", DataType.ETypeCode.Int32),
                new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal),
                new TableColumn("DateColumn", DataType.ETypeCode.DateTime),
                new TableColumn("SortColumn", DataType.ETypeCode.Int32)
                });

            table.Data.Add(new object[] { "value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10 });
            table.Data.Add(new object[] { "value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9 });
            table.Data.Add(new object[] { "value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), 8 });
            table.Data.Add(new object[] { "value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), 7 });
            table.Data.Add(new object[] { "value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6 });
            table.Data.Add(new object[] { "value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5 });
            table.Data.Add(new object[] { "value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4 });
            table.Data.Add(new object[] { "value08", 8, 8.1, Convert.ToDateTime("2015/01/08"), 3 });
            table.Data.Add(new object[] { "value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2 });
            table.Data.Add(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1 });

            SourceTable Adapter = new SourceTable(table, new List<Sort>() { new Sort("StringColumn") } );
            Adapter.ResetValues();
            return Adapter;
        }

        public static SourceTable CreateUnSortedTestData()
        {
            Table table = new Table("test", new List<TableColumn>() {
                new TableColumn("StringColumn", DataType.ETypeCode.String),
                new TableColumn("IntColumn", DataType.ETypeCode.Int32),
                new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal),
                new TableColumn("DateColumn", DataType.ETypeCode.DateTime),
                new TableColumn("SortColumn", DataType.ETypeCode.Int32),
                new TableColumn("GroupColumn", DataType.ETypeCode.String)
                });

            table.Data.Add(new object[] { "value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10, "Odd" });
            table.Data.Add(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1, "Even" });
            table.Data.Add(new object[] { "value03", 3, 3.1, Convert.ToDateTime("2015/01/03"), 8, "Odd" });
            table.Data.Add(new object[] { "value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9, "Even" });
            table.Data.Add(new object[] { "value08", 8, 8.1, Convert.ToDateTime("2015/01/08"), 3, "Even" });
            table.Data.Add(new object[] { "value04", 4, 4.1, Convert.ToDateTime("2015/01/04"), 7, "Even" });
            table.Data.Add(new object[] { "value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4, "Odd" });
            table.Data.Add(new object[] { "value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6, "Odd" });
            table.Data.Add(new object[] { "value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5, "Even" });
            table.Data.Add(new object[] { "value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2, "Odd" });

            SourceTable Adapter = new SourceTable(table, null);
            Adapter.ResetValues();
            return Adapter;
        }

        public static SourceTable CreateSortedJoinData()
        {
            Table table = new Table("test", new List<TableColumn>() {
                new TableColumn("StringColumn", DataType.ETypeCode.String),
                new TableColumn("IntColumn", DataType.ETypeCode.Int32),
                new TableColumn("LookupValue", DataType.ETypeCode.String)
                });

            table.Data.Add(new object[] { "value01", 1, "lookup1" });
            table.Data.Add(new object[] { "value02", 2, "lookup2" });
            table.Data.Add(new object[] { "value03", 3, "lookup3" });
            table.Data.Add(new object[] { "value04", 4, "lookup4" });
            table.Data.Add(new object[] { "value05", 5, "lookup5" });
            table.Data.Add(new object[] { "value06", 6, "lookup6" });
            table.Data.Add(new object[] { "value07", 7, "lookup7" });
            table.Data.Add(new object[] { "value08", 8, "lookup8" });
            table.Data.Add(new object[] { "value09", 9, "lookup9" });

            SourceTable Adapter = new SourceTable(table, new List<Sort>() { new Sort("StringColumn") });
            Adapter.ResetValues();
            return Adapter;
        }

        public static SourceTable CreateUnSortedJoinData()
        {
            Table table = new Table("test", new List<TableColumn>() {
                new TableColumn("StringColumn", DataType.ETypeCode.String),
                new TableColumn("IntColumn", DataType.ETypeCode.Int32),
                new TableColumn("LookupValue", DataType.ETypeCode.String)
                });

            table.Data.Add(new object[] { "value09", 9, "lookup9" });
            table.Data.Add(new object[] { "value06", 6, "lookup6" });
            table.Data.Add(new object[] { "value01", 1, "lookup1" });
            table.Data.Add(new object[] { "value05", 5, "lookup5" });
            table.Data.Add(new object[] { "value02", 2, "lookup2" });
            table.Data.Add(new object[] { "value04", 4, "lookup4" });
            table.Data.Add(new object[] { "value03", 3, "lookup3" });
            table.Data.Add(new object[] { "value07", 7, "lookup7" });
            table.Data.Add(new object[] { "value08", 8, "lookup8" });

            SourceTable Adapter = new SourceTable(table);
            Adapter.ResetValues();
            return Adapter;
        }

        public static SourceTable CreateLargeTable(int rows)
        {
            object[] row;
            Table table = new Table("test");

            for (int i = 0; i < 10; i++)
                table.AddColumn("column" + i.ToString(), DataType.ETypeCode.Int32);

            table.AddColumn("random", DataType.ETypeCode.String);

            for (int i = 0; i < rows; i++)
            {
                row = new object[11];

                for (int j = 0; j < 10; j++)
                {
                    row[j] = j;
                }

                row[10] = Guid.NewGuid().ToString();

                table.Data.Add(row);
            }

            return new SourceTable(table);
        }
    }
}
