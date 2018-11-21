using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using OfficeOpenXml;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

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

        public ReaderExcel(Connection connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;
        }

        public override void Close()
        {
            _excelPackage?.Dispose();
        }

        public override Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            try
            {
                if (_isOpen)
                {
                    throw new ConnectionException("The excel file is already open.");
                }

                var connection = ((ConnectionExcel)ReferenceConnection);
                _excelPackage = connection.NewConnection();
                _currentRow = connection.ExcelDataRow;
                _excelWorkSheet = connection.GetWorkSheet(_excelPackage, CacheTable.Name);

                _query = query;

                // get the position of each of the column names.
                _columnMappings = new Dictionary<int, (int ordinal, TableColumn column)>();
                var headerRow = ((ConnectionExcel)ReferenceConnection).ExcelHeaderRow;
                var headerCol = ((ConnectionExcel)ReferenceConnection).ExcelHeaderCol;
                var headerMaxCol = ((ConnectionExcel)ReferenceConnection).ExcelHeaderColMax;

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

                _headerOrdinals = ((ConnectionExcel)ReferenceConnection).GetHeaderOrdinals(_excelWorkSheet);

                _isOpen = true;
                _excelWorkSheetRows = _excelWorkSheet.Dimension.Rows;

                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to open the Excel reader for sheet {CacheTable.Name}.  {ex.Message}.", ex);
            }
        }

        public override string Details()
        {
            return "Excel Database Service";
        }

        public override bool ResetTransform()
        {
            _currentRow = ((ConnectionExcel) ReferenceConnection).ExcelDataRow;
            return true;
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            try
            {
                if(!_isOpen)
                {
                    throw new ConnectionException("The excel file has not been opened");
                }

                if(_currentRow > _excelWorkSheetRows || _currentRow > ((ConnectionExcel) ReferenceConnection).ExcelDataRowMax )
                {
                    return Task.FromResult<object[]>(null);
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
                                return Task.FromResult<object[]>(null);
                            }
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                var row = new object[_columnMappings.Count];

				foreach (var mapping in _columnMappings)
				{
				    var value = _excelWorkSheet.GetValue(_currentRow, mapping.Key);
                    row[mapping.Value.Ordinal] = ((ConnectionExcel)ReferenceConnection).ParseExcelValue(value, mapping.Value.Column);
				}
				    
				_currentRow++;
                return Task.FromResult(row);

            }
            catch (Exception ex)
            {
                throw new Exception("The read record failed due to the following error: " + ex.Message, ex);
            }
        }

        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("Direct lookup not supported with excel files.");
        }
    }
}
