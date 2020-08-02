using System;
using dexih.functions;
using Dexih.Utils.DataType;
using Xunit;
using Xunit.Abstractions;

namespace dexih.transforms.tests
{
    public class ReaderMemoryTests
    {
        private readonly ITestOutputHelper _output;

        public ReaderMemoryTests(ITestOutputHelper output)
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

            return table;
        }

        private object[] NewRow()
        {
            return new object[] {"string", 3, 3.1, DateTime.Now, 1, null};
        }

        
        /// <summary>
        ///  Simple performance test that checks how loading, reading of readermemory table.
        /// </summary>
        /// <param name="iterations"></param>
        [Theory]
        [InlineData(1000000)]
        public void Performance_ReaderMemory(int iterations)
        {
            var table = CreateSampleTable();
            
            var time = TaskTimer.Start(() =>
            {
                for (var i = 0; i < iterations; i++)
                {
                    table.AddRow(NewRow());
                }
            });

            var reader = new ReaderMemory(table);
            reader.Open().Wait();

            _output.WriteLine($"memory reader add rows: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");

            var count = 0;
            
            time = TaskTimer.Start(() =>
            {
                while(reader.Read())
                {
                    count++;
                }
                
                Assert.Equal(iterations, count);
            });
            
            _output.WriteLine($"memory reader read rows: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");

            reader.Reset();
            reader.Open();
            count = 0;
            
            time = TaskTimer.Start(() =>
            {
                while(reader.Read())
                {
                    Assert.Equal("string", reader["StringColumn"]);
                    Assert.Equal(3, reader["IntColumn"]);
                    Assert.Equal(3.1, reader["DecimalColumn"]);
                    count++;
                }
                
                Assert.Equal(iterations, count);
            });
            
            // _output.WriteLine($"memory reader read with colunm names and check values rows: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");
            
            reader.Reset();
            reader.Open();
            count = 0;
            
            time = TaskTimer.Start(() =>
            {
                while(reader.Read())
                {
                    Assert.Equal("string", reader[0]);
                    Assert.Equal(3, reader[1]);
                    Assert.Equal(3.1, reader[2]);
                    count++;
                }
                
                Assert.Equal(iterations, count);
            });
            
            _output.WriteLine($"memory reader read with ordinals and check values rows: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");
            
            reader.Reset();
            reader.Open();
            count = 0;
            
            time = TaskTimer.Start(() =>
            {
                while(reader.Read())
                {
                    Assert.Equal("string", reader.GetValue<string>(0));
                    Assert.Equal(3, reader.GetInt32(1));
                    Assert.Equal(3.1, reader.GetDouble(2));
                    count++;
                }
                
                Assert.Equal(iterations, count);
            });
            
            _output.WriteLine($"memory reader read convert datatype and check values rows: {iterations}, time taken: {time}, iterations/ms: {iterations/time.Milliseconds}");

        }
    }
}