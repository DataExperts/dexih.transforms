using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using static dexih.functions.DataType;
using Newtonsoft.Json.Linq;
using System.Threading;
using OfficeOpenXml;

namespace dexih.connections.excel
{
    public class ReaderExcelDatabase : Transform
    {
        private bool _isOpen = false;
        private ExcelPackage _excelPackage;
        private ExcelWorksheet _excelWorkSheet;
        private int _excelWorkSheetRows;
		private int _excelWorkSheetColumns;
        private int _currentRow;

        public ReaderExcelDatabase(Connection connection, Table table, Transform referenceTransform)
        {
            ReferenceConnection = connection;
            CacheTable = table;
            ReferenceTransform = referenceTransform;
        }

        protected override void Dispose(bool disposing)
        {
            _isOpen = false;

            if(_excelPackage != null)
            {
                _excelPackage.Dispose();
            }

            base.Dispose(disposing);
        }

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query)
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
                    _excelPackage = ((ConnectionExcelDatabase)ReferenceConnection).NewConnection();
                    _currentRow = 1;

                    _excelWorkSheet = _excelPackage.Workbook.Worksheets.SingleOrDefault(c => c.Name == CacheTable.TableName);
                    if (_excelWorkSheet == null)
                    {
                        return new ReturnValue<Table>(false, $"The worksheet {query.Table} could not be found in the excel file. ", null);
                    }

					_isOpen = true;
                    _excelWorkSheetRows = _excelWorkSheet.Dimension.Rows;
					_excelWorkSheetColumns = _excelWorkSheet.Dimension.Columns;

                    return new ReturnValue(true);
                });
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

                _currentRow++;

                if(_currentRow > _excelWorkSheetRows)
                {
                    return new ReturnValue<object[]>(false, null);
                }

                var row = new object[_excelWorkSheetColumns];

                return await Task.Run(() =>
				{

				    for (int col = 1; col <= _excelWorkSheetColumns; col++)
				    {
				        row[col-1] = _excelWorkSheet.Cells[_currentRow, col].Value.ToString();
				    }

				    return new ReturnValue<object[]>(true, row);
				});
                   
            }
            catch (Exception ex)
            {
                throw new Exception("The read record failed due to the following error: " + ex.Message, ex);
            }
        }

        public override bool CanLookupRowDirect { get; } = false;
    }
}
