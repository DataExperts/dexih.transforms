using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.File
{
    public class FileHandlerText : FileHandlerBase, IDisposable
    {
        private Table _table;
        private SelectQuery _selectQuery;
        private readonly FileConfiguration _fileConfiguration;

        private CsvReader _csvReader;
        private Stream _stream;

        private readonly int _fileRowNumberOrdinal;
        private readonly int _responseDataOrdinal;

        private struct CsvField
        {
            public DataType.ETypeCode TypeCode { get; set; }
            public int Position { get; set; }
            public Type DataType { get; set; }

            public CsvField(int position, DataType.ETypeCode typeCode, bool isArray)
            {
                Position = position;
                TypeCode = typeCode;

                if (isArray
                    || typeCode == Dexih.Utils.DataType.DataType.ETypeCode.Binary
                    || typeCode == Dexih.Utils.DataType.DataType.ETypeCode.Byte
                    || typeCode == Dexih.Utils.DataType.DataType.ETypeCode.Char
                    || typeCode == Dexih.Utils.DataType.DataType.ETypeCode.Guid
                    || typeCode == Dexih.Utils.DataType.DataType.ETypeCode.Json
                    || typeCode == Dexih.Utils.DataType.DataType.ETypeCode.Unknown
                    || typeCode == Dexih.Utils.DataType.DataType.ETypeCode.Xml)
                {
                    DataType = typeof(string);
                }
                else
                {
                    DataType = Dexih.Utils.DataType.DataType.GetType(typeCode);    
                }
                
            }
        }
        
        private Dictionary<int, CsvField> _csvOrdinalMappings;

        private int _currentFileRowNumber;

        
        public FileHandlerText(Table table, FileConfiguration fileConfiguration)
        {
            _table = table;

            if (fileConfiguration == null)
            {
                _fileConfiguration = new FileConfiguration();
            }
            else
            {
                _fileConfiguration = fileConfiguration;
                _fileConfiguration.Delimiter = _fileConfiguration.Delimiter.Replace("\\t", "\t").Replace("\\n", "\n").Replace("\\r", "\r");
            }
            
            
            _fileRowNumberOrdinal = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.FileRowNumber);
            _responseDataOrdinal = _table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData);
        }

        public override async Task<ICollection<TableColumn>> GetSourceColumns(Stream stream)
        {
            var streamReader = new StreamReader(stream);
            
            //skip the header records as specified.
            for (var i = 0; i < _fileConfiguration.SkipHeaderRows && !streamReader.EndOfStream; i++)
            {
                await streamReader.ReadLineAsync();
            }
            
            string[] headers;
            if (_fileConfiguration.HasHeaderRecord)
            {
                try
                {
                    using (var csv = new CsvReader(streamReader, _fileConfiguration))
                    {
                        await csv.ReadAsync();
                        csv.ReadHeader();
                        headers = csv.Context.HeaderRecord;
                    }
                }
                catch (Exception ex)
                {
                    throw new FileHandlerException($"Error occurred opening the file stream: {ex.Message}", ex);
                }
            }
            else
            {
                // if no header row specified, then just create a series column names "column001, column002 ..."
                using (var csv = new CsvReader(streamReader, _fileConfiguration))
                {
                    await csv.ReadAsync();
                    headers = Enumerable.Range(0, csv.Context.HeaderRecord.Length).Select(c => "column-" + c.ToString().PadLeft(3, '0')).ToArray();
                }
            }

            var columns = new List<TableColumn>();

            foreach (var field in headers)
            {
                var col = new TableColumn()
                {

                    //add the basic properties
                    Name = field,
                    LogicalName = field,
                    IsInput = false,
                    DataType = DataType.ETypeCode.String,
                    DeltaType = TableColumn.EDeltaType.TrackingField,
                    Description = "",
                    AllowDbNull = true,
                    IsUnique = false
                };
                columns.Add(col);
            }

            columns.Add(new TableColumn()
            {

                //add the basic properties
                Name = "FileRow",
                LogicalName = "FileRow",
                IsInput = false,
                DataType = DataType.ETypeCode.Int32,
                DeltaType = TableColumn.EDeltaType.FileRowNumber,
                Description = "The file row number the record came from.",
                AllowDbNull = false,
                IsUnique = false
            });

            return columns;
        }

        public override async Task SetStream(Stream stream, SelectQuery selectQuery)
        {
            _stream = stream;
            _currentFileRowNumber = 0;
            _selectQuery = selectQuery;
            var streamReader = new StreamReader(stream);

            if (_fileConfiguration != null)
            {
                //skip the header records as specified.
                for (var i = 0; i < _fileConfiguration.SkipHeaderRows && !streamReader.EndOfStream; i++)
                {
                    await streamReader.ReadLineAsync();
                }

                _csvReader = new CsvReader(streamReader, _fileConfiguration);
            }
            else 
            {
                _csvReader = new CsvReader(streamReader);
            }
            
            _csvOrdinalMappings = new Dictionary<int,CsvField>();

            // create mappings from column name positions, to the csv field name positions.
            if(_fileConfiguration != null && ( _fileConfiguration.MatchHeaderRecord || !_fileConfiguration.HasHeaderRecord))
            {
                await _csvReader.ReadAsync();
                _csvReader.ReadHeader();

                for(var col = 0; col < _table.Columns.Count; col++)
                {
                    var column = _table.Columns[col];
                    if (column.DeltaType != TableColumn.EDeltaType.FileName && column.DeltaType != TableColumn.EDeltaType.FileRowNumber)
                    {
                        for (var csvPos = 0; csvPos < _csvReader.Context.HeaderRecord.Length; csvPos++)
                        {
                            if (_csvReader.Context.HeaderRecord[csvPos] == column.Name)
                            {
                                _csvOrdinalMappings.Add(col, new CsvField(csvPos, column.DataType, column.Rank > 0));
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                // not matching in header, just create mappings equal
                for (var col = 0; col < _table.Columns.Count; col++)
                {
                    var column = _table.Columns[col];
                    if (column.DeltaType != TableColumn.EDeltaType.FileName && column.DeltaType != TableColumn.EDeltaType.FileRowNumber)
                    {
                        _csvOrdinalMappings.Add(col, new CsvField(col, column.DataType, column.Rank > 0));
                    }
                }
            }

        }

        public override async Task<object[]> GetRow(object[] baseRow)
        {
            try
            {
                while (_csvReader != null && await _csvReader.ReadAsync())
                {
                    var row = new object[baseRow.Length];
                    Array.Copy(baseRow, row, baseRow.Length);

                    if (_responseDataOrdinal >= 0)
                    {
                        row[_responseDataOrdinal] = _csvReader.Context.RawRecord;
                    }

                    _currentFileRowNumber++;

                    foreach (var colPos in _csvOrdinalMappings.Keys)
                    {
                        var mapping = _csvOrdinalMappings[colPos];
                        var value = _csvReader.GetField(mapping.DataType, mapping.Position);
                        row[colPos] = DataType.TryParse(mapping.TypeCode, value);
                        
                        if (_fileConfiguration.SetWhiteSpaceCellsToNull && row[colPos] is string &&
                            string.IsNullOrWhiteSpace((string) row[colPos]))
                        {
                            row[colPos] = null;
                        }
                    }

                    if (_fileRowNumberOrdinal >= 0)
                    {
                        row[_fileRowNumberOrdinal] = _currentFileRowNumber;
                    }

                    if (_selectQuery == null || _selectQuery.EvaluateRowFilter(row, _table))
                    {
                        return row;
                    }
                }
                return null;
            }
            catch (Exception ex)
            {
                throw new FileHandlerException($"The file load failed at data row {_currentFileRowNumber}, with error - {ex.Message}", ex);
            }
        }

        public override void Dispose()
        {
            _csvReader?.Dispose();
            _stream?.Dispose();
        }
    }
}