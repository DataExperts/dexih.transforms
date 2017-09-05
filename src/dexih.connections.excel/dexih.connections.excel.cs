using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.transforms;
using dexih.functions;
using System.IO;
using System.Data.Common;
using System.Diagnostics;
using System.Threading;
using static dexih.functions.DataType;
using OfficeOpenXml;
using OfficeOpenXml.FormulaParsing.Utilities;

namespace dexih.connections.excel
{
    
    public class ConnectionExcel : Connection
    {
	    public readonly int ExcelHeaderRow = 1;
	    public readonly int ExcelHeaderCol = 1;
	    public readonly int ExcelHeaderColMax = 16384;
	    public readonly int ExcelDataRow = 2;
	    public readonly int ExcelDataRowMax = 1048576;
	    
        public override string ServerHelp => "The full path containing the excel file";
        public override string DefaultDatabaseHelp => "Excel File";
        public override bool AllowNtAuth => false;
        public override bool AllowUserPass => false;
        public override bool CanBulkLoad => false;
        public override bool CanSort => false;
        public override bool CanFilter => false;
        public override bool CanDelete => true;
        public override bool CanUpdate => true;
        public override bool CanAggregate => false;
	    public override bool CanUseBinary => true;
	    public override bool CanUseSql => false;
        public override bool DynamicTableCreation => false;


        public override string DatabaseTypeName => "Excel Database";
        public override ECategory DatabaseCategory => ECategory.SqlDatabase;

	    public override object GetDataTypeMaxValue(ETypeCode typeCode, int length = 0)
	    {
		    // Note: Excel only support max 15 digits.
		    switch (typeCode)
		    {
			    case ETypeCode.DateTime:
				    return new DateTime(9999,12,31,23,59,59,999); 
			    case ETypeCode.UInt64:
				    return (ulong)999999999999999; 
			    case ETypeCode.Int64:
				    return (long)999999999999999; 
			    case ETypeCode.Decimal:
				    return (decimal)999999999999999; 
			    default:
				    return DataType.GetDataTypeMaxValue(typeCode, length);
		    }
	    }
	    
	    public override object GetDataTypeMinValue(ETypeCode typeCode)
	    {
		    // Note: Excel only support max 15 digits.
		    switch (typeCode)
		    {
			    case ETypeCode.DateTime:
				    return new DateTime(1900,01,01,0,0,0,0); 
			    case ETypeCode.Int64:
				    return (long)-999999999999999; 
			    case ETypeCode.Decimal:
				    return (decimal)-999999999999999; 
			    default:
				    return DataType.GetDataTypeMinValue(typeCode);
		    }
		    
	    }
	    
        public override async Task<ReturnValue> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
	        var package = NewConnection();
	        
	        var tableExistsResult = await TableExists(table, cancelToken);
	        if (tableExistsResult.Value)
	        {
		        if (dropTable)
		        {
			        package.Workbook.Worksheets.Delete(table.Name);
		        }
		        else
		        {
			        return new ReturnValue(false, $"The worksheet {table.Name} already exists in the Excel file", null);
		        }
	        }
	        var sheet = package.Workbook.Worksheets.Add(table.Name);
	        
	        // Add column headings
	        for (var i = 0; i < table.Columns.Count; i++)
	        {
		        var column = table.Columns[i];
		        sheet.SetValue(ExcelHeaderRow, i+1, column.Name);
		        switch (column.Datatype)
		        {
					case ETypeCode.DateTime:
						sheet.Column(i+1).Style.Numberformat.Format = "yyyy-mm-dd";
						break;
		        }
	        }

	        package.Save();
	        return new ReturnValue(true);
        }

		public override async Task<ReturnValue<List<string>>> GetDatabaseList(CancellationToken cancelToken)
		{
			try
			{
				var directoryExists = await Task.Run(() => Directory.Exists(Server), cancelToken);
				if (!directoryExists)
				{
					return new ReturnValue<List<string>>(false, "The directory " + Server + " does not exist.", null);
				}

				var dbList = await Task.Factory.StartNew(() =>
				{
					var files = Directory.GetFiles(Server, "*.xlsx");
					return files.Select(Path.GetFileName).ToList();
				}, cancelToken);

				return new ReturnValue<List<string>>(true, "", null, dbList);
			}
			catch (Exception ex)
			{
				return new ReturnValue<List<string>>(false, "The excel files could not be listed due to the following error: " + ex.Message, ex, null);
			}
		}

        public ExcelPackage NewConnection() 
        {
            var path = new FileInfo(Path.Combine(Server, DefaultDatabase??""));
            var package = new ExcelPackage(path);
            return package;
        }

		public override async Task<ReturnValue<List<Table>>> GetTableList(CancellationToken cancelToken)
		{
            try
            {
                return await Task.Run(() =>
                {

                    using (var package = NewConnection())
                    {
                        var tableList = new List<Table>();

                        foreach (var worksheet in package.Workbook.Worksheets)
                        {
                            var table = new Table(worksheet.Name);
                            tableList.Add(table);
                        }

                        return new ReturnValue<List<Table>>(true, tableList);
                    }
                }, cancelToken);            
            }
            catch(Exception ex)
            {
                return new ReturnValue<List<Table>>(false, "The excel file could not be opened due to:" + ex.Message, ex);
            }
		}

	    /// <summary>
	    /// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
	    /// </summary>
	    /// <param name="importTable"></param>
	    /// <param name="cancelToken"></param>
	    /// <returns></returns>
	    public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table importTable, CancellationToken cancelToken)
        {
            try
            {
                return await Task.Run(() =>
				{

				    using (var package = NewConnection())
				    {
				        var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == importTable.Name);
				        if (worksheet == null)
				        {
				            return new ReturnValue<Table>(false, $"The worksheet {importTable.Name} could not be found in the excel file. ", null);
				        }

				        var columns = new TableColumns();
				        for (var col = ExcelHeaderCol; col <= worksheet.Dimension.Columns && col <= ExcelHeaderColMax; col++)
				        {
				            var columName = worksheet.Cells[ExcelHeaderRow, col].Value.ToString();
				            if (string.IsNullOrEmpty(columName)) columName = "Column-" + col.ToString();
				            var column = new TableColumn(columName, ETypeCode.String);
					        
					        //search the data to determine datatype.
					        var datatype = ETypeCode.Unknown;
					        for (var row = ExcelDataRow; row <= worksheet.Dimension.Rows; row++)
					        {
						        var value = worksheet.GetValue(row, col);
								if (datatype == ETypeCode.Unknown || datatype == ETypeCode.DateTime)
								{
									if (worksheet.Cells[row, col].Style.Numberformat.Format.Contains("yy"))
									{
										datatype = ETypeCode.DateTime;
										continue;
									}
									if (datatype == ETypeCode.DateTime)
									{
										datatype = ETypeCode.Int64;
									}

								}
						        
						        if (datatype == ETypeCode.Unknown || datatype == ETypeCode.Int64)
						        {
                                    if(value.IsNumeric())
                                    {
                                        if(Math.Abs((Double)value % 1) <= (Double.Epsilon * 100))
                                        {
                                            datatype = ETypeCode.Int64;
                                            continue;
                                        }
                                        else
                                        {
                                            datatype = ETypeCode.Double;
                                            continue;
                                        }
                                    }
									datatype = ETypeCode.String;
							        break;
						        }

						        if (datatype == ETypeCode.Unknown || datatype == ETypeCode.Decimal)
						        {
									if (value.IsNumeric())
									{
										datatype = ETypeCode.Decimal;
								        continue;
							        }
							        datatype = ETypeCode.String;
							        break;
							        
						        }
						        datatype = ETypeCode.String;
						        break;

					        }

                            column.Datatype = datatype == ETypeCode.Unknown ? ETypeCode.String : datatype;
					        columns.Add(column);
				        }

				        var newTable = new Table(importTable.Name, -1, columns);
				        return new ReturnValue<Table>(true, newTable);
				    }
				}, cancelToken);

            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The following error was encountered importing the excel sheet: " + ex.Message, ex);
            }
        }
	    
        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
	        return await Task.Run(() =>
	        {

		        using (var package = NewConnection())
		        {
			        var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == table.Name);
			        if (worksheet == null)
			        {
				        return new ReturnValue<Table>(false, $"The worksheet {table.Name} could not be found in the excel file. ", null);
			        }

			        for (var row = ExcelDataRow; row <= worksheet.Dimension.Rows && row <= ExcelDataRowMax; row++)
			        {
				        worksheet.DeleteRow(ExcelDataRow);
			        }

			        package.Save();
			        
			        return new ReturnValue(true);
		        }
	        }, cancelToken);        
        }
	    

        public override async Task<ReturnValue<Table>> InitializeTable(Table table, int position)
        {
            return await Task.Run(() => new ReturnValue<Table>(true, table));
        }

        public override async Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
	        return await Task.Run(() =>
	        {
		        var rowsUpdated = 0;
		        
		        using (var package = NewConnection())
		        {
			        var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == table.Name);
			        if (worksheet == null)
			        {
				        return new ReturnValue<long>(true,
					        $"The worksheet {table.Name} could not be found in the excel file. ", null);
			        }
			        
			        var columnMappings = GetHeaderOrdinals(worksheet);

			        // Scan through the excel sheet, checking the update queries for each row.
			        for (var row = ExcelDataRow; row < worksheet.Dimension.Rows || row < ExcelDataRowMax; row++)
			        {
				        // check if any of the queries apply to this row.
				        foreach (var query in queries)
				        {
					        var updateResult = EvaluateRowFilter(worksheet, row, columnMappings, query.Filters);
					        if (updateResult)
					        {
						        if (query.UpdateColumns != null)
						        {
							        // update the row with each of specified column values.
							        foreach (var updateColumn in query.UpdateColumns)
							        {
								        if (updateColumn.Column != null)
								        {
									        if (!columnMappings.ContainsKey(updateColumn.Column.Name))
									        {
										        return new ReturnValue<long>(true,
											        $"The column {updateColumn.Column.Name} could not be found on the Excel sheet.", null);
									        }
									        worksheet.SetValue(row, columnMappings[updateColumn.Column.Name], updateColumn.Value);
								        }
							        }
						        }
						        rowsUpdated++;
						        break;
					        }   
				        }
			        }

			        if (rowsUpdated > 0)
			        {
				        package.Save();
			        }

			        // if the row was not found, return null
			        return new ReturnValue<long>(true, rowsUpdated);

		        }
	        }, cancelToken);          
        }
	    

        public override async Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancelToken)
        {
     		return await Task.Run(() =>
	        {
		        var rowsDeleted = 0;
		        
		        using (var package = NewConnection())
		        {
			        var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == table.Name);
			        if (worksheet == null)
			        {
				        return new ReturnValue<long>(true,
					        $"The worksheet {table.Name} could not be found in the excel file. ", null);
			        }
			        
			        var columnMappings = GetHeaderOrdinals(worksheet);

			        // Scan through the excel sheet, checking the update queries for each row.
			        for (var row = ExcelDataRow; row < worksheet.Dimension.Rows || row < ExcelDataRowMax; row++)
			        {
				        // check if any of the queries apply to this row.
				        foreach (var query in queries)
				        {
					        var deleteResult = EvaluateRowFilter(worksheet, row, columnMappings, query.Filters);
					        if (deleteResult)
					        {
						        worksheet.DeleteRow(row);
						        rowsDeleted++;
						        row--; //move the row count back one as the current row has been deleted.
						        break;
					        }   
				        }
			        }

			        if (rowsDeleted > 0)
			        {
				        package.Save();
			        }

			        // if the row was not found, return null
			        return new ReturnValue<long>(true, rowsDeleted);

		        }
	        }, cancelToken);
	        
        }

	    /// <summary>
	    /// Creates a dictionary with the name and position of each column heading.
	    /// </summary>
	    /// <param name="worksheet"></param>
	    /// <returns></returns>
	    public Dictionary<string, int> GetHeaderOrdinals(ExcelWorksheet worksheet)
	    {
		    // get the position of each of the column names.
		    var columnMappings = new Dictionary<string, int>();
		    for (var col = ExcelHeaderCol; col <= worksheet.Dimension.Columns && col <= ExcelHeaderColMax; col++)
		    {
			    var columName = worksheet.GetValue(ExcelHeaderRow, col).ToString();
			    if (string.IsNullOrEmpty(columName)) columName = "Column-" + col.ToString();
			    columnMappings.Add(columName, col);
		    }

		    return columnMappings;
	    }
	    
	    public DateTime FromExcelSerialDate(double d)
	    {
		    if (!(d >= 0))
			    throw new ArgumentOutOfRangeException(); // NaN or negative d not supported

//		    if (d > 59) //Excel/Lotus 2/29/1900 bug   
//		    {
//			    d -= 1;
//		    }

		    var ticks = Convert.ToInt64(d * TimeSpan.TicksPerDay);
		    ticks = ticks - (ticks % TimeSpan.TicksPerMillisecond); //trim sub-milliseconds as excel doesn't support

		    var theDate = (new DateTime(1899, 12, 30)) + TimeSpan.FromTicks(ticks);

		    return theDate;
	    }
	
	    public TimeSpan FromExcelSerialTime(double d)
	    {
		    if (!(d >= 0))
			    throw new ArgumentOutOfRangeException(); // NaN or negative d not supported

		    var ticks = Convert.ToInt64(d * TimeSpan.TicksPerDay);
		    ticks = ticks - (ticks % TimeSpan.TicksPerMillisecond);  //trim sub-milliseconds as excel doesn't support

		    return TimeSpan.FromTicks(ticks);
	    }
	    
	    public ReturnValue<object> ParseExcelValue(object value, TableColumn column)
	    {
		    object parsedValue;
		    if (value is Double && column.Datatype == DataType.ETypeCode.DateTime)
		    {
			    parsedValue = FromExcelSerialDate((Double) value);
		    } else if (value is Double && column.Datatype == ETypeCode.Time)
		    {
			    parsedValue = FromExcelSerialTime((Double) value);
		    }
		    else
		    {
			    var parseResult = DataType.TryParse(column.Datatype, value, column.MaxLength);
			    if (!parseResult.Success)
			    {
				    return new ReturnValue<object>(false,
					    $"Read failed due to conversion error with {column.Name} value {value.ToString()}. Message: {parseResult.Message}",
					    parseResult.Exception);
			    }
			    parsedValue = parseResult.Value;
		    }
		    return new ReturnValue<object>(true, parsedValue);
	    }

	    public bool EvaluateRowFilter(ExcelWorksheet worksheet, int row, IReadOnlyDictionary<string, int> headerOrdinals, List<Filter> filters)
	    {
		    if (filters == null)
		    {
			    return true;
		    }
		    
		    var filterResult = true;
		    var isFirst = true;
				        
		    foreach (var filter in filters)
		    {
			    var column1Value = filter.Column1 == null
				    ? null
				    : worksheet.GetValue(row, headerOrdinals[filter.Column1.Name]);
			    var column2Value = filter.Column2 == null
				    ? null
				    : worksheet.GetValue(row, headerOrdinals[filter.Column2.Name]);

			    if (isFirst)
			    {
				    filterResult = filter.Evaluate(column1Value, column2Value);
				    isFirst = false;
			    }
			    else if (filter.AndOr == Filter.EAndOr.And)
			    {
				    filterResult = filterResult && filter.Evaluate(column1Value, column2Value);
			    }
			    else
			    {
				    filterResult = filterResult || filter.Evaluate(column1Value, column2Value);
			    }
		    }

		    return filterResult;
	    }


        public override async Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
	        var timer = Stopwatch.StartNew();

	        return await Task.Run(() =>
	        {
		        long rowsInserted = 0;
		        
		        using (var package = NewConnection())
		        {
			        var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == table.Name);
			        if (worksheet == null)
			        {
				        return new ReturnValue<Tuple<long, long>>(true,
					        $"The worksheet {table.Name} could not be found in the excel file. ", null);
			        }

			        var columnMappings = GetHeaderOrdinals(worksheet);

			        var autoIncrementColumn = table.GetDeltaColumn(TableColumn.EDeltaType.AutoIncrement);
			        var autoIncrementOrdinal = -1;
			        if(autoIncrementColumn != null && columnMappings.ContainsKey(autoIncrementColumn.Name))
			        {
				        autoIncrementOrdinal = columnMappings[autoIncrementColumn.Name];
			        }
			        long autoIncrementValue = -1;

			        var row = worksheet.Dimension.Rows;
			        if (row < ExcelDataRow)
			        {
				        row = ExcelDataRow;
			        }

			        foreach (var query in queries)
			        {
				        if (row > ExcelDataRowMax)
				        {
					        return new ReturnValue<Tuple<long, long>>(false, $"The row count has exceeded the maximum of ${ExcelDataRowMax}.", null);
				        }
				        
				        if (cancelToken.IsCancellationRequested)
					        return new ReturnValue<Tuple<long, long>>(false, "Insert rows cancelled.", null,
						        Tuple.Create(timer.Elapsed.Ticks, (long) 0));

				        if (autoIncrementOrdinal >= 0)
				        {
					        autoIncrementValue = row;
					        worksheet.SetValue(row, autoIncrementOrdinal, autoIncrementValue);
				        }

				        foreach (var column in query.InsertColumns)
				        {
					        if (!columnMappings.ContainsKey(column.Column.Name))
					        {
						        return new ReturnValue<Tuple<long, long>>(false, $"The column with the name ${column.Column.Name} could not be found.", null);
					        }
					        worksheet.SetValue(row, columnMappings[column.Column.Name], column.Value);
				        }
				        rowsInserted++;
				        row++;
			        }

			        if (rowsInserted > 0)
			        {
				        package.Save();
			        }
			        
			        return new ReturnValue<Tuple<long, long>>(true, Tuple.Create(timer.Elapsed.Ticks, autoIncrementValue));
		        }
	        }, cancelToken);
        }

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
	        var timer = Stopwatch.StartNew();

	        return await Task.Run(() =>
	        {
		        using (var package = NewConnection())
		        {
			        var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == table.Name);
			        if (worksheet == null)
			        {
				        return new ReturnValue<object>(true,
					        $"The worksheet {table.Name} could not be found in the excel file. ", null);
			        }

			        if (query.Columns == null || query.Columns.Count == 0)
			        {
				        return new ReturnValue<object>(true,
					        $"The query contained no columns. ", null);
			        
			        }
			        
			        var columnMappings = GetHeaderOrdinals(worksheet);

			        for (var row = ExcelDataRow; row < worksheet.Dimension.Rows || row < ExcelDataRowMax; row++)
			        {
				        var filterResult = EvaluateRowFilter(worksheet, row, columnMappings, query.Filters);
				        if (filterResult)
				        {
					        var column = query.Columns[0];

					        if (!columnMappings.ContainsKey(column.Column.Name))
					        {
						        return new ReturnValue<object>(true,
							        $"The column ${column.Column.Name} could not be found on the Excel sheet.", null);
					        }
					        var value = worksheet.GetValue(row, columnMappings[column.Column.Name]);
					        var parsedValue = ParseExcelValue(value, column.Column);
					        if (!parsedValue.Success)
					        {
						        return new ReturnValue<object>(parsedValue);
					        }
					        return new ReturnValue<object>(true, parsedValue.Value);
				        }
			        }

			        // if the row was not found, return null
			        return new ReturnValue<object>(true, null);

		        }
	        }, cancelToken);        
        }

        public override async Task<ReturnValue> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
	        return await Task.Run(() =>
	        {
		        if (!databaseName.EndsWith(".xlsx"))
		        {
			        DefaultDatabase = databaseName + ".xlsx";
		        }
		        else
		        {
			        DefaultDatabase = databaseName;
		        }
		        var package = NewConnection();
		        return package == null ? new ReturnValue(false) : new ReturnValue(true);
	        }, cancelToken);
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancelToken)
        {
			var timer = Stopwatch.StartNew();

			using (var package = NewConnection())
			{
				long rowsInserted = 0;
				
				var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == table.Name);
				if (worksheet == null)
				{
					return new ReturnValue<long>(true, $"The worksheet {table.Name} could not be found in the excel file. ", null);
				}
				
				// get the position of each of the column names.
				var columnMappings = GetHeaderOrdinals(worksheet);
				var row = worksheet.Dimension.Rows+1;

				while(await reader.ReadAsync(cancelToken))
				{
					if (cancelToken.IsCancellationRequested)
						return new ReturnValue<long>(false, "Insert rows cancelled.", null, timer.ElapsedTicks);

					foreach (var mapping in columnMappings)
					{
						worksheet.SetValue(row, mapping.Value, reader[mapping.Key]);
					}

					row++;
					rowsInserted++;
				}

				if (rowsInserted > 0)
				{
					package.Save();
				}
				return new ReturnValue<long>(true, timer.ElapsedTicks);
			}
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderExcel(this, table, referenceTransform);
            return reader;
        }

        public override async Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
	        return await Task.Run(() =>
	        {
		        using (var package = NewConnection())
		        {
			        var worksheet = package.Workbook.Worksheets.SingleOrDefault(c => c.Name == table.Name);
			        if (worksheet == null)
			        {
				        return new ReturnValue<bool>(true, $"The worksheet {table.Name} could not be found in the excel file. ", null, false);
			        }

			        return new ReturnValue<bool>(true, true);

		        }
	        }, cancelToken);      
        }


    }
}
