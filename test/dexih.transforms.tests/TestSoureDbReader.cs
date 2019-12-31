using System;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Xunit;

namespace dexih.transforms.tests
{
    public class TestSoureDbReader
    {
        [Fact]
        public async Task sourceDbReader_UnitTest()
        {
            var connection = new SqliteConnection("Data Source=:memory:;");
            connection.Open();

            var cmd = new SqliteCommand("CREATE TABLE [test_data]([StringColumn] VARCHAR(100) PRIMARY KEY NOT NULL,[IntColumn] INT,[DateColumn] DATETIME);", connection);
            cmd.ExecuteNonQuery();

            for (var i = 0; i < 10; i++)
            {
                var sql = "INSERT INTO [test_data] values ('value" + i.ToString().PadLeft(2, '0') + "', " + i + ", '2001-01-" + (i+1).ToString().PadLeft(2, '0') + "');";
                cmd = new SqliteCommand(sql, connection);
                cmd.ExecuteNonQuery();
            }

            cmd = new SqliteCommand("select * from [test_data]", connection);
            var reader = cmd.ExecuteReader();

            //run tests with no cache.
            var dbReader = new ReaderDbDataReader(reader);
            dbReader.SetCacheMethod(ECacheMethod.NoCache);
            await dbReader.Open();

            //check the fields load correctly
            Assert.Equal("StringColumn", dbReader.GetName(0));
            Assert.Equal("IntColumn", dbReader.GetName(1));
            Assert.Equal("DateColumn", dbReader.GetName(2));

            var count = 0;
            while(await dbReader.ReadAsync())
            {
                Assert.Equal("value" + count.ToString().PadLeft(2, '0'), dbReader["StringColumn"]);
                Assert.Equal(count, Convert.ToInt32(dbReader["IntColumn"]));
                Assert.Equal(Convert.ToDateTime("2001-01-" + (count + 1).ToString().PadLeft(2, '0')) ,Convert.ToDateTime(dbReader["DateColumn"]));
                count++;
            }

            cmd = new SqliteCommand("select * from [test_data]", connection);
            reader = cmd.ExecuteReader();

            //run tests with pre-load cache.
            dbReader = new ReaderDbDataReader(reader);
            dbReader.SetCacheMethod(ECacheMethod.DemandCache);
            await dbReader.Open();

            //check the fields load correctly
            Assert.Equal("StringColumn", dbReader.GetName(0));
            Assert.Equal("IntColumn", dbReader.GetName(1));
            Assert.Equal("DateColumn", dbReader.GetName(2));

            count = 0;
            while (await dbReader.ReadAsync())
            {
                Assert.Equal("value" + count.ToString().PadLeft(2, '0'), dbReader["StringColumn"]);
                Assert.Equal(count, Convert.ToInt32(dbReader["IntColumn"]));
                Assert.Equal(Convert.ToDateTime("2001-01-" + (count + 1).ToString().PadLeft(2, '0')), Convert.ToDateTime(dbReader["DateColumn"]));
                count++;
            }

            Assert.Equal(10, count);

            //reset the reader and re-test using the cache.
            dbReader.SetRowNumber(0);
            count = 0;
            while (await dbReader.ReadAsync())
            {
                Assert.Equal("value" + count.ToString().PadLeft(2, '0'), dbReader["StringColumn"]);
                Assert.Equal(count, Convert.ToInt32(dbReader["IntColumn"]));
                Assert.Equal(Convert.ToDateTime("2001-01-" + (count + 1).ToString().PadLeft(2, '0')), Convert.ToDateTime(dbReader["DateColumn"]));
                count++;
            }

            Assert.Equal(10, count);


            //peek at a row
            var peekRow = new object[3];
            dbReader.RowPeek(5, peekRow);
            Assert.Equal("value05", peekRow[0]);
            Assert.Equal(Convert.ToInt32(5), Convert.ToInt32(peekRow[1]));
            Assert.Equal(Convert.ToDateTime("2001-01-06"), Convert.ToDateTime(peekRow[2])); 


        }


    }
}
