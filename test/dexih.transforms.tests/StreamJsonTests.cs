using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.File;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.transforms.tests
{
    public class StreamJsonTests
    {
        [Fact]
        public async Task StreamJson_GetSourceColumns_Test()
        {
            var stream = System.IO.File.OpenRead("Data/weather.json");
            var table = new WebService() {MaxImportLevels = 2};

            var handler = new FileHandlerJson(table, null);
            var columns = (await handler.GetSourceColumns(stream)).ToArray();

            Assert.Equal("coord", columns[0].ColumnGroup);
            Assert.Equal("lon", columns[0].Name);
            Assert.Equal(DataType.ETypeCode.Double, columns[1].DataType);
            Assert.Equal("coord", columns[1].ColumnGroup);
            Assert.Equal("lat", columns[1].Name);
            Assert.Equal(DataType.ETypeCode.Double, columns[1].DataType);

            Assert.Equal("weather", columns[2].Name);
            Assert.Equal(DataType.ETypeCode.Node, columns[2].DataType);
            Assert.Equal("id", columns[2].ChildColumns[0].Name);
            Assert.Equal(DataType.ETypeCode.Int32, columns[2].ChildColumns[0].DataType);
            Assert.Equal("main", columns[2].ChildColumns[1].Name);
            Assert.Equal(DataType.ETypeCode.String, columns[2].ChildColumns[1].DataType);
            Assert.Equal("description", columns[2].ChildColumns[2].Name);
            Assert.Equal(DataType.ETypeCode.String, columns[2].ChildColumns[2].DataType);
            Assert.Equal("icon", columns[2].ChildColumns[3].Name);
            Assert.Equal(DataType.ETypeCode.String, columns[2].ChildColumns[3].DataType);

        }    
        
        [Fact]
        public async Task StreamJson_ReadRow_Test()
        {
            var stream = System.IO.File.OpenRead("Data/weather.json");
            var table = new WebService {MaxImportLevels = 2};

            var handler = new FileHandlerJson(table, null);
            var columns = (await handler.GetSourceColumns(stream)).ToArray();
            table.Columns = new TableColumns(columns);
            
            stream = System.IO.File.OpenRead("Data/weather.json");
            handler = new FileHandlerJson(table, null);

            await handler.SetStream(stream, null);

            var row = await handler.GetRow();

            // coords are a property, so will return an object.
            Assert.Equal(51.51, row[table.GetOrdinal("coord.lat")]);
            Assert.Equal(-0.13, row[table.GetOrdinal("coord.lon")]);

            // weather is an array, so will return a transform.
            var weatherCol = table["weather"];
            var weatherValue = (Transform) row[table.GetOrdinal(weatherCol)];

            Assert.True(await weatherValue.ReadAsync());
            Assert.Equal(300, weatherValue["id"]);
            Assert.Equal("Drizzle", weatherValue["main"]);
            Assert.Equal("light intensity drizzle", weatherValue["description"]);
            Assert.Equal("09d", weatherValue["icon"]);
            Assert.False(await weatherValue.ReadAsync());
            
            Assert.Equal("stations", row[table.GetOrdinal("base")]);
        } 
        
        [Fact]
        public async Task StreamJson_GetSourceColumns_Array_Test()
        {
            var stream = System.IO.File.OpenRead("Data/array.json");
            var table = new WebService();

            var handler = new FileHandlerJson(table, null);
            var columns = (await handler.GetSourceColumns(stream)).ToArray();

            Assert.Equal("name", columns[0].Name);
            Assert.Equal(DataType.ETypeCode.String, columns[0].DataType);
            Assert.Equal("age", columns[1].Name);
            Assert.Equal(DataType.ETypeCode.Int32, columns[1].DataType);

        } 
        
        [Fact]
        public async Task StreamJson_ReadArray_Test()
        {
            var stream = System.IO.File.OpenRead("Data/array.json");
            var table = new WebService();

            var handler = new FileHandlerJson(table, null);
            var columns = (await handler.GetSourceColumns(stream)).ToArray();
            table.Columns = new TableColumns(columns);
            
            stream = System.IO.File.OpenRead("Data/array.json");
            handler = new FileHandlerJson(table, null);

            await handler.SetStream(stream, null);

            
            var row = await handler.GetRow();
            Assert.Equal("Harry", row[table.GetOrdinal("name")]);
            Assert.Equal(10, row[table.GetOrdinal("age")]);
            Assert.Equal(new [] {1,2,3}, row[table.GetOrdinal("numbers")]);

            row = await handler.GetRow();
            Assert.Equal("Ron", row[table.GetOrdinal("name")]);
            Assert.Equal(11, row[table.GetOrdinal("age")]);

            row = await handler.GetRow();
            Assert.Equal("Hermione", row[table.GetOrdinal("name")]);
            Assert.Equal(10, row[table.GetOrdinal("age")]);

            row = await handler.GetRow();
            Assert.Null(row);
        } 
    }
}