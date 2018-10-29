using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.BuiltIn;
using dexih.functions.Mappings;
using dexih.functions.Parameter;
using dexih.functions.Query;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.transforms.tests
{
    public class TransformSeriesTests
    {
        private readonly string _seriesFunctions = typeof(SeriesFunctions).FullName;

        [Fact]
        public async Task Group_SeriesTests1()
        {
            var table = new Table("test", 0,
                new TableColumn("StringColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("IntColumn", DataType.ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DateColumn", DataType.ETypeCode.DateTime, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("SortColumn", DataType.ETypeCode.Int32, TableColumn.EDeltaType.TrackingField)
            );

            // data with gaps in the date sequence.
            table.AddRow("value01", 1, 1.1, Convert.ToDateTime("2015/01/01"), 10 );
            table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9 );
            table.AddRow("value05", 5, 5.1, Convert.ToDateTime("2015/01/05"), 6 );
            table.AddRow("value06", 6, 6.1, Convert.ToDateTime("2015/01/06"), 5 );
            table.AddRow("value07", 7, 7.1, Convert.ToDateTime("2015/01/07"), 4 );
            table.AddRow("value09", 9, 9.1, Convert.ToDateTime("2015/01/09"), 2 );
            table.AddRow("value10", 10, 10.1, Convert.ToDateTime("2015/01/10"), 1);

            var source = new ReaderMemory(table, new List<Sort> { new Sort("StringColumn") } );
            source.Reset();

            var mappings = new Mappings(false);

            var mavg = Functions.GetFunction(_seriesFunctions, nameof(SeriesFunctions.MovingAverage)).GetTransformFunction(typeof(double));
            
            var parameters = new Parameters()
            {
                Inputs = new Parameter[]
                {
                    new ParameterColumn("IntColumn", DataType.ETypeCode.Double),
                    new ParameterValue("Aggregate", DataType.ETypeCode.Unknown, SelectColumn.EAggregate.Sum), 
                },
                ResultInputs = new Parameter[]
                {
                    new ParameterValue("PreCount", DataType.ETypeCode.Int32, 3),
                    new ParameterValue("PostCount", DataType.ETypeCode.Int32, 3)
                },
                ResultReturnParameter = new ParameterOutputColumn("MAvg", DataType.ETypeCode.Double)
            };
            
            mappings.Add(new MapFunction(mavg, parameters));
            mappings.Add(new MapSeries(new TableColumn("DateColumn"), ESeriesGrain.Day, true, null, null));
            
            var transformGroup = new TransformSeries(source, mappings);
            
            Assert.Equal(2, transformGroup.FieldCount);

            var counter = 0;
            double[] mAvgExpectedValues = { 0.75, 1.6, 2.33, 3, 2.86, 3.86, 5.29, 6.17, 6.4, 6.5 };
            string[] expectedDates = { "2015/01/01", "2015/01/02", "2015/01/03", "2015/01/04", "2015/01/05", "2015/01/06", "2015/01/07", "2015/01/08", "2015/01/09", "2015/01/10" };
            while (await transformGroup.ReadAsync())
            {
                Assert.Equal(mAvgExpectedValues[counter], Math.Round((double)transformGroup["MAvg"], 2));
                Assert.Equal(Convert.ToDateTime(expectedDates[counter]), transformGroup["DateColumn"]);
                counter = counter + 1;
            }
            Assert.Equal(10, counter);
        }
        
        [Fact]
        public async Task Group_SeriesTests_DuplicateRows()
        {
            var table = new Table("test", 0,
                new TableColumn("StringColumn", DataType.ETypeCode.String, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("IntColumn", DataType.ETypeCode.Int32, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DecimalColumn", DataType.ETypeCode.Decimal, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("DateColumn", DataType.ETypeCode.DateTime, TableColumn.EDeltaType.NaturalKey),
                new TableColumn("SortColumn", DataType.ETypeCode.Int32, TableColumn.EDeltaType.TrackingField)
            );

            // data with duplicates
            table.AddRow("value01", 2, 1.1, Convert.ToDateTime("2015/01/01"), 10 );
            table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9 );
            table.AddRow("value02", 2, 2.1, Convert.ToDateTime("2015/01/02"), 9 );

            var source = new ReaderMemory(table, new List<Sort> { new Sort("StringColumn") } );
            source.Reset();

            var mappings = new Mappings(false);

            var mavg = Functions.GetFunction(_seriesFunctions, nameof(SeriesFunctions.MovingAverage)).GetTransformFunction(typeof(double));
            
            var parameters = new Parameters()
            {
                Inputs = new Parameter[]
                {
                    new ParameterColumn("IntColumn", DataType.ETypeCode.Double),
                    new ParameterValue("Aggregate", DataType.ETypeCode.Unknown, SelectColumn.EAggregate.Sum), 
                },
                ResultInputs = new Parameter[]
                {
                    new ParameterValue("PreCount", DataType.ETypeCode.Int32, 3),
                    new ParameterValue("PostCount", DataType.ETypeCode.Int32, 3)
                },
                ResultReturnParameter = new ParameterOutputColumn("MAvg", DataType.ETypeCode.Double)
            };
            
            mappings.Add(new MapFunction(mavg, parameters));

            mappings.Add(new MapSeries(new TableColumn("DateColumn"), ESeriesGrain.Day, false, null, null));
            
            var transformGroup = new TransformSeries(source, mappings);

            var counter = 0;
            double[] mAvgExpectedValues = { 3, 3 };
            string[] expectedDates = { "2015/01/01", "2015/01/02",  };
            while (await transformGroup.ReadAsync())
            {
                Assert.Equal(mAvgExpectedValues[counter], Math.Round((double)transformGroup["MAvg"], 2));
                Assert.Equal(Convert.ToDateTime(expectedDates[counter]), transformGroup["DateColumn"]);
                counter = counter + 1;
            }
            Assert.Equal(2, counter);        
        }

        [Fact]
        public async Task Group_SeriesTests2()
        {
            var source = Helpers.CreateSortedTestData();

            //add a row to test highest since.
            source.Add(new object[] { "value11", 5, 10.1, Convert.ToDateTime("2015/01/11"), 1, new[] { 1, 1 } });
            source.Reset();

            var mappings = new Mappings(true);

            var mavg = Functions.GetFunction(_seriesFunctions, nameof(SeriesFunctions.MovingAverage)).GetTransformFunction(typeof(double));
            
            var parameters = new Parameters()
            {
                Inputs = new Parameter[]
                {
                    new ParameterColumn("IntColumn", DataType.ETypeCode.Double),
                    new ParameterValue("Aggregate", DataType.ETypeCode.Unknown, SelectColumn.EAggregate.Sum), 
                },
                ResultInputs = new Parameter[]
                {
                    new ParameterValue("PreCount", DataType.ETypeCode.Int32, 3),
                    new ParameterValue("PostCount", DataType.ETypeCode.Int32, 3)
                },
                ResultReturnParameter = new ParameterOutputColumn("MAvg", DataType.ETypeCode.Double)
            };
            
            mappings.Add(new MapFunction(mavg, parameters));
            
            var highest = Functions.GetFunction(_seriesFunctions, nameof(SeriesFunctions.HighestSince)).GetTransformFunction(typeof(double));
            parameters = new Parameters()
            {
                Inputs = new Parameter[]
                {
                    new ParameterColumn("IntColumn", DataType.ETypeCode.Double),
                    new ParameterValue("Aggregate", DataType.ETypeCode.Unknown, SelectColumn.EAggregate.Sum), 
                },
                ResultOutputs = new Parameter[]
                {
                    new ParameterOutputColumn("Count", DataType.ETypeCode.Int32), 
                    new ParameterOutputColumn("HighestValue", DataType.ETypeCode.Double), 
                },
                ResultReturnParameter = new ParameterOutputColumn("Highest", DataType.ETypeCode.DateTime)
            };
            mappings.Add(new MapFunction(highest, parameters));
            mappings.Add(new MapSeries(new TableColumn("DateColumn"), ESeriesGrain.Day, false, null, null));

            var transformGroup = new TransformSeries(source, mappings);
            Assert.Equal(10, transformGroup.FieldCount);

            var counter = 0;
            double[] mAvgExpectedValues = { 2.5, 3, 3.5, 4, 5, 6, 7, 7.14, 7.5, 7.8, 8 };
            string[] highestExpectedValues = { "2015/01/01", "2015/01/02", "2015/01/03", "2015/01/04", "2015/01/05", "2015/01/06", "2015/01/07", "2015/01/08", "2015/01/09", "2015/01/10", "2015/01/10" };
            while (await transformGroup.ReadAsync())
            {
                Assert.Equal(mAvgExpectedValues[counter], Math.Round((double)transformGroup["MAvg"], 2));
                Assert.Equal(highestExpectedValues[counter], ((DateTime)transformGroup["Highest"]).ToString("yyyy/MM/dd"));
                counter = counter + 1;
            }
            Assert.Equal(11, counter);
        }
    }
}