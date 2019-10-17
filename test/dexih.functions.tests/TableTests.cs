using System.Linq;
using Dexih.Utils.DataType;
using Xunit;
using Xunit.Abstractions;

namespace dexih.functions.tests
{
    public class TableTests
    {
        private readonly ITestOutputHelper _output;

        public TableTests(ITestOutputHelper output)
        {
            _output = output;
        }
        
        private Table CreateSampleTable()
        {
            var table = new Table("test", 0,
                new TableColumn("StringColumn", ETypeCode.String, EDeltaType.NaturalKey),
                new TableColumn("IntColumn", ETypeCode.Int32, EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", ETypeCode.Decimal, EDeltaType.NaturalKey),
                new TableColumn("DateColumn", ETypeCode.DateTime, EDeltaType.NaturalKey),
                new TableColumn("SortColumn", ETypeCode.Int32, EDeltaType.TrackingField),
                new TableColumn("ArrayColumn", ETypeCode.Int32, EDeltaType.TrackingField, 1)
            );
            table.AddAuditColumns();

            return table;
        }

        [Theory]
        [InlineData(1000000)]
        public void Performance_Table_Ordinal_Name(int iterations)
        {
            var table = CreateSampleTable();
            var name = "DateColumn";

            var time = TaskTimer.Start(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var ordinal = -1;
                    for (var col = 0; col < table.Columns.Count; col++)
                    {
                        if (table.Columns[col].Name == name)
                        {
                            ordinal = col;
                            break;
                        }
                    }
                    Assert.Equal(3, ordinal);
                }
            });

            _output.WriteLine($"non-optimized column lookup - iterations: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");
            
            time = TaskTimer.Start(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var ordinal = table.GetOrdinal(name);
                    Assert.Equal(3, ordinal);
                }
            });
            
            _output.WriteLine($"column lookup - iterations: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");
        }
        
        
        [Theory]
        [InlineData(1000000)]
        public void Performance_Table_Ordinal_DeltaType(int iterations)
        {
            var table = CreateSampleTable();
            var deltaType = EDeltaType.ValidToDate;

            var time = TaskTimer.Start(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var ordinal = -1;
                    for (var col = 0; col < table.Columns.Count; col++)
                    {
                        if (table.Columns[col].DeltaType == deltaType)
                        {
                            ordinal = col;
                            break;
                        }
                    }
                    Assert.Equal(7, ordinal);
                }
            });

            _output.WriteLine($"non-optimized delta lookup - iterations: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");
            
            time = TaskTimer.Start(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var ordinal = table.GetOrdinal(deltaType);
                    Assert.Equal(7, ordinal);
                }
            });
            
            _output.WriteLine($"column delta - iterations: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");
        }
        
        [Theory]
        [InlineData(1000000)]
        public void Performance_Table_NaturalKeys(int iterations)
        {
            var table = CreateSampleTable();

            var time = TaskTimer.Start(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var cols = table.Columns.Where(c => c.DeltaType == EDeltaType.TrackingField).ToArray();
                    Assert.Equal(2, cols.Length);
                }
            });

            _output.WriteLine($"non-optimized column lookup - iterations: {iterations}, time taken: {time}, iterations/ms: {(time.Milliseconds == 0 ? 0 : (iterations/time.Milliseconds))}");
            
            time = TaskTimer.Start(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    var cols = table.GetColumns(EDeltaType.TrackingField);
                    Assert.Equal(2, cols.Length);
                }
            });
            
            _output.WriteLine($"column lookup - iterations: {iterations}, time taken: {time}, iterations/ms: {(time.Milliseconds == 0 ? 0 : iterations/time.Milliseconds)}");
        }
    }
}