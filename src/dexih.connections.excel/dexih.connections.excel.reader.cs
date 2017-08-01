using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using OfficeOpenXml;

namespace dexih.connections.excel
{
    public class ReaderExcel : Transform
    {
        private bool _isOpen = false;
        private ExcelPackage _excelPackage;
        private ExcelWorksheet _excelWorkSheet;
        private int _excelWorkSheetRows;
        private int _currentRow;
        private SelectQuery _query;

        private Dictionary<int, (int Ordinal, TableColumn Column)> _columnMappings;
        private Dictionary<string, int> _headerOrdinals;

        public ReaderExcel(Connection connection, Table table, Transform referenceTransform)
        {
            ReferenceConnection = connection;
            CacheTable = table;
            ReferenceTransform = referenceTransform;
        }

        protected override void Dispose(bool disposing)
        {
            _excelPackage?.Dispose();
            base.Dispose(disposing);
        }

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;
            try
            {
                if (_isOpen)
                {
                    return new ReturnValue(false, "The excel file is already open.", null);
                }

                return await Task.Run(() =>
                {
                    _excelPackage = ((ConnectionExcel)ReferenceConnection).NewConnection();
                    _currentRow = ((ConnectionExcel) ReferenceConnection).ExcelDataRow;

                    _excelWorkSheet = _excelPackage.Workbook.Worksheets.SingleOrDefault(c => c.Name == CacheTable.Name);
                    if (_excelWorkSheet == null)
                    {
                        return new ReturnValue<Table>(false, $"The worksheet {query.Table} could not be found in the excel file. ", null);
                    }

                    _query = query;

                    // get the position of each of the column names.
                    _columnMappings = new Dictionary<int, (int ordinal, TableColumn column)>();
                    var headerRow = ((ConnectionExcel) ReferenceConnection).ExcelHeaderRow;
                    var headerCol = ((ConnectionExcel) ReferenceConnection).ExcelHeaderCol;
                    var headerMaxCol = ((ConnectionExcel) ReferenceConnection).ExcelHeaderColMax;
                    
                    for (var col = headerCol; col <= _excelWorkSheet.Dimension.Columns && col <= headerMaxCol; col++)
                    {
                        var columName = _excelWorkSheet.Cells[headerRow, col].Value.ToString();
                        if (string.IsNullOrEmpty(columName)) columName = "Column-" + col;
                        var column = CacheTable.Columns[columName];
                        var ordinal = CacheTable.GetOrdinal(columName);

                        if (ordinal >= 0 && column != null)
                        {
                            _columnMappings.Add(col, (ordinal, column));
                        }
                    }

                    _headerOrdinals = ((ConnectionExcel) ReferenceConnection).GetHeaderOrdinals(_excelWorkSheet);
                    
                    _isOpen = true;
                    _excelWorkSheetRows = _excelWorkSheet.Dimension.Rows;

                    return new ReturnValue(true);
                }, cancelToken);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when opening the excel file: " + ex.Message, ex);
            }
        }

        public override string Details()
        {
            return "Excel Database Service";
        }

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override ReturnValue ResetTransform()
        {
            _currentRow = ((ConnectionExcel) ReferenceConnection).ExcelDataRow;
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            try
            {
                if(!_isOpen)
                {
                    return new ReturnValue<object[]>(false, "The read record failed as the excel file is not open.", null);
                }

                if(_currentRow > _excelWorkSheetRows || _currentRow > ((ConnectionExcel) ReferenceConnection).ExcelDataRowMax )
                {
                    return new ReturnValue<object[]>(false, null);
                }

                if (_query?.Filters != null)
                {
                    while (true)
                    {
                        var filterResult = ((ConnectionExcel) ReferenceConnection).EvaluateRowFilter(_excelWorkSheet,
                            _currentRow, _headerOrdinals, _query.Filters);

                        if (!filterResult)
                        {
                            _currentRow++;
                            if (_currentRow > _excelWorkSheetRows ||
                                _currentRow > ((ConnectionExcel) ReferenceConnection).ExcelDataRowMax)
                            {
                                return new ReturnValue<object[]>(false, null);
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                var row = new object[_columnMappings.Count];

                return await Task.Run(() =>
				{
				    foreach (var mapping in _columnMappings)
				    {
				        var value = _excelWorkSheet.GetValue(_currentRow, mapping.Key);

				        var parsedValue = ((ConnectionExcel) ReferenceConnection).ParseExcelValue(value, mapping.Value.Column);
				        if (!parsedValue.Success)
				        {
				            return new ReturnValue<object[]>(parsedValue);
				        }
				        row[mapping.Value.Ordinal] = parsedValue.Value;
				    }
				    
				    _currentRow++;
				    return new ReturnValue<object[]>(true, row);

				}, cancellationToken);


            }
            catch (Exception ex)
            {
                throw new Exception("The read record failed due to the following error: " + ex.Message, ex);
            }
        }

        public override bool CanLookupRowDirect { get; } = false;
    }
}
