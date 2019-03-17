using dexih.functions;
using dexih.transforms;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.test
{
    public static class Configuration
    {
        public static int counter = 1;

        public static IConfigurationSection AppSettings { get; set; }

        static Configuration()
        {
            Console.WriteLine("Current directory is: " + Directory.GetCurrentDirectory());

            try
            {
                var builder = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .AddUserSecrets<Startup>()
                    .AddEnvironmentVariables();

                var configuration = builder.Build();
                AppSettings = configuration.GetSection("AppSettings");

            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }

        public static string EncryptionKey()
        {
            return AppSettings["EncryptionKey"];
        }

        public static string TestExportsPath()
        {
            //return "C:\\Users\\Gary\\OneDrive\\Information Hub\\tests";
            //return "Z:\\OneDrive\\Information Hub\\tests";

            return Directory.GetCurrentDirectory() + "\\testfiles";
        }

        public static ReaderMemory CreateTestData()
        {
            Table table = new Table("test" + (counter++).ToString(), 0,
                new TableColumn("StringColumn", ETypeCode.String),
                new TableColumn("IntColumn", ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal),
                new TableColumn("DateColumn", ETypeCode.DateTime),
                new TableColumn("GuidColumn", ETypeCode.Guid)
                );

            table.Data.Add(new object[] { "value1", 1, 1.1, Convert.ToDateTime("2015/01/01"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value2", 2, 2.1, Convert.ToDateTime("2015/01/02"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value3", 3, 3.1, Convert.ToDateTime("2015/01/03"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value4", 4, 4.1, Convert.ToDateTime("2015/01/04"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value5", 5, 5.1, Convert.ToDateTime("2015/01/05"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value6", 6, 6.1, Convert.ToDateTime("2015/01/06"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value7", 7, 7.1, Convert.ToDateTime("2015/01/07"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value8", 8, 8.1, Convert.ToDateTime("2015/01/08"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value9", 9, 9.1, Convert.ToDateTime("2015/01/09"), Guid.NewGuid() });
            table.Data.Add(new object[] { "value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), Guid.NewGuid() });

            ReaderMemory Adapter = new ReaderMemory(table);
            Adapter.Reset();
            return Adapter;
        }

        public static Table CreateTable()
        {
            Table table = new Table("testtable" + (counter++).ToString())
            {
                Description = "The testing table"
            };

            table.Columns.Add(new TableColumn()
            {
                Name = "StringColumn",
                Description = "A string column",
                DataType = ETypeCode.String,
                DeltaType = TableColumn.EDeltaType.TrackingField
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "IntColumn",
                Description = "An integer column",
                DataType = ETypeCode.Int32,
                DeltaType = TableColumn.EDeltaType.NaturalKey
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "DecimalColumn",
                Description = "A decimal column",
                DataType = ETypeCode.Decimal,
                DeltaType = TableColumn.EDeltaType.TrackingField,
                Scale = 2,
                Precision = 10
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "DateColumn",
                Description = "A date column column",
                DataType = ETypeCode.DateTime,
                DeltaType = TableColumn.EDeltaType.TrackingField
            });

            table.Columns.Add(new TableColumn()
            {
                Name = "GuidColumn",
                Description = "A guid column",
                DataType = ETypeCode.Guid,
                DeltaType = TableColumn.EDeltaType.TrackingField
            });

            return table;
        }
    }

    public class Startup 
    {
        
    }
}
