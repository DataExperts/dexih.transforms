using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using dexih.transforms;
using dexih.functions;
using System.IO;
using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Threading;
using static dexih.functions.DataType;
using OfficeOpenXml;

namespace dexih.connections.excel
{
    
    public class ConnectionExcelDatabase : Connection
    {
        public override string ServerHelp => "The full path containing the excel file";
        public override string DefaultDatabaseHelp => "Excel File";
        public override bool AllowNtAuth => false;
        public override bool AllowUserPass => false;
        public override bool CanBulkLoad => false;
        public override bool CanSort => false;
        public override bool CanFilter => false;
        public override bool CanAggregate => false;

        public override string DatabaseTypeName => "Excel Database";
        public override ECategory DatabaseCategory => ECategory.SqlDatabase;

        public override Task<ReturnValue> CreateTable(Table table, bool dropTable = false)
        {
            throw new NotImplementedException();
        }

		public override async Task<ReturnValue<List<string>>> GetDatabaseList()
		{
			try
			{
				bool directoryExists = await Task.Run(() => Directory.Exists(Server));
				if (!directoryExists)
					return new ReturnValue<List<string>>(false, "The directory " + Server + " does not exist.", null);

				var dbList = await Task.Factory.StartNew(() =>
				{
					var files = Directory.GetFiles(Server, "*.xlsx");

					List<string> list = new List<string>();

					foreach (var file in files)
					{
						list.Add(Path.GetFileName(file));
					}

					return list;
				});

				return new ReturnValue<List<string>>(true, "", null, dbList);
			}
			catch (Exception ex)
			{
				return new ReturnValue<List<string>>(false, "The excel files could not be listed due to the following error: " + ex.Message, ex, null);
			}
		}

        public ExcelPackage NewConnection() 
        {
            var path = new FileInfo(Path.Combine(Server, DefaultDatabase));
            var package = new ExcelPackage(path);
            return package;
        }

		public override async Task<ReturnValue<List<Table>>> GetTableList()
		{
            try
            {
                return await Task.Run(() =>
                {

                    using (ExcelPackage package = NewConnection())
                    {
                        var tableList = new List<Table>();

                        foreach (var worksheet in package.Workbook.Worksheets)
                        {
                            var table = new Table(worksheet.Name);
                            tableList.Add(table);
                        }

                        return new ReturnValue<List<Table>>(true, tableList);
                    }
                });            
            }
            catch(Exception ex)
            {
                return new ReturnValue<List<Table>>(false, "The excel file could not be opened due to:" + ex.Message, ex);
            }
		}

        /// <summary>
        /// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <param name="Properties">Mandatory property "RestfulUri".  Additional properties for the default column values.  Use ColumnName=value</param>
        /// <returns></returns>
         public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table importTable)
        {
            try
            {
                return await Task.Run(() =>
				{

				    using (ExcelPackage package = NewConnection())
				    {
				        var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == importTable.TableName);
				        if (worksheet == null)
				        {
				            return new ReturnValue<Table>(false, $"The worksheet {importTable.TableName} could not be found in the excel file. ", null);
				        }

				        var columns = new TableColumns();
				        var headerRow = worksheet.Row(1);
				        for (int col = 1; col <= worksheet.Dimension.Columns; col++)
				        {
				            var columName = worksheet.Cells[1, col].Value.ToString();
				            if (string.IsNullOrEmpty(columName)) columName = "Column-" + col.ToString();
				            var column = new TableColumn(columName, ETypeCode.String);
				            columns.Add(column);
				        }

				        var newTable = new Table(importTable.TableName, -1, columns);
				        return new ReturnValue<Table>(true, newTable);
				    }
				});

            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The following error was encountered importing the excel sheet: " + ex.Message, ex);
            }
        }


        public override Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue> AddMandatoryColumns(Table table, int position)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public override Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue> CreateDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query = null)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null )
        {
            var reader = new ReaderExcelDatabase(this, table, referenceTransform);
            return reader;
        }

        public override Task<ReturnValue<bool>> TableExists(Table table)
        {
            throw new NotImplementedException();
        }


    }
}
