using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms.File
{
    public class FileHandlerText : FileHandlerBase
    {
        private readonly Table _table;
        private readonly FileConfiguration _fileConfiguration;

        private CsvReader _csvReader;
        private Stream _stream;

        private readonly int _fileRowNumberOrdinal;
        private readonly int _responseDataOrdinal;
        private readonly int _fieldCount;

        public override string FileType { get; } = "Csv";
        public SelectQuery SelectQuery { get; set; }

        private struct CsvField
        {
            public ETypeCode TypeCode { get; set; }
            public int Rank { get; set; }
            public int Position { get; set; }
            public Type DataType { get; set; }

            public CsvField(int position, ETypeCode typeCode, int rank)
            {
                Position = position;
                TypeCode = typeCode;
                Rank = rank;

                if (rank > 0
                    || typeCode == ETypeCode.Binary
                    || typeCode == ETypeCode.Byte
                    || typeCode == ETypeCode.CharArray
                    || typeCode == ETypeCode.Guid
                    || typeCode == ETypeCode.Json
                    || typeCode == ETypeCode.Unknown
                    || typeCode == ETypeCode.Xml)
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
            _fieldCount = _table.Columns.Count;

            if (fileConfiguration == null)
            {
                _fileConfiguration = new FileConfiguration();
            }
            else
            {
                _fileConfiguration = fileConfiguration;
                _fileConfiguration.Delimiter = _fileConfiguration.Delimiter.Replace("\\t", "\t").Replace("\\n", "\n").Replace("\\r", "\r");
            }
            
            _fileRowNumberOrdinal = table.GetOrdinal(EDeltaType.FileRowNumber);
            _responseDataOrdinal = _table.GetOrdinal(EDeltaType.ResponseData);
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
            AvailableDataTypes[] dataTypes;
            using (var csv = new CsvReader(streamReader, _fileConfiguration))
            {
                if (_fileConfiguration.HasHeaderRecord)
                {
                    try
                    {
                        await csv.ReadAsync();
                        csv.ReadHeader();
                        headers = csv.Context.HeaderRecord;
                    }
                    catch (Exception ex)
                    {
                        throw new FileHandlerException($"Error occurred opening the file stream: {ex.Message}", ex);
                    }
                }
                else
                {
                    await csv.ReadAsync();
                    headers = Enumerable.Range(0, csv.Context.HeaderRecord.Length)
                        .Select(c => "column-" + c.ToString().PadLeft(3, '0')).ToArray();
                }
            
                //read the records to infer datatypes
                dataTypes = new AvailableDataTypes[headers.Length];
                while (await csv.ReadAsync())
                {
                    for(var i = 0; i< headers.Length; i++)
                    {
                        dataTypes[i].CheckValue(csv[i]);
                    }
                }
            }

            var columns = new List<TableColumn>();

            for(var i = 0; i< headers.Length; i++)
            {
                var header = headers[i];
                var col = new TableColumn()
                {
                    //add the basic properties
                    Name = header,
                    LogicalName = header,
                    IsInput = false,
                    DataType = dataTypes[i].GetBestType(),
                    DeltaType = EDeltaType.TrackingField,
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
                DataType = ETypeCode.Int32,
                DeltaType = EDeltaType.FileRowNumber,
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
            SelectQuery = selectQuery;
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
                _csvReader = new CsvReader(streamReader, CultureInfo.CurrentCulture);
            }
            
            _csvOrdinalMappings = new Dictionary<int,CsvField>();

            // create mappings from column name positions, to the csv field name positions.
            if(_fileConfiguration != null && ( _fileConfiguration.MatchHeaderRecord && _fileConfiguration.HasHeaderRecord))
            {
                await _csvReader.ReadAsync();
                _csvReader.ReadHeader();

                for(var col = 0; col < _table.Columns.Count; col++)
                {
                    var column = _table.Columns[col];
                    if (column.DeltaType != EDeltaType.FileName && column.DeltaType != EDeltaType.FileRowNumber)
                    {
                        for (var csvPos = 0; csvPos < _csvReader.Context.HeaderRecord.Length; csvPos++)
                        {
                            if (_csvReader.Context.HeaderRecord[csvPos] == column.Name)
                            {
                                _csvOrdinalMappings.Add(col, new CsvField(csvPos, column.DataType, column.Rank));
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
                    if (column.DeltaType != EDeltaType.FileName && column.DeltaType != EDeltaType.FileRowNumber)
                    {
                        _csvOrdinalMappings.Add(col, new CsvField(col, column.DataType, column.Rank));
                    }
                }
            }
        }

        public override async Task<object[]> GetRow()
        {
            try
            {
                while (_csvReader != null && await _csvReader.ReadAsync())
                {
                    var row = new object[_fieldCount];

                    if (_responseDataOrdinal >= 0)
                    {
                        row[_responseDataOrdinal] = _csvReader.Context.RawRecord;
                    }

                    _currentFileRowNumber++;

                    foreach (var colPos in _csvOrdinalMappings.Keys)
                    {
                        var mapping = _csvOrdinalMappings[colPos];
                        var value = _csvReader[mapping.Position];
                        if (string.IsNullOrEmpty(value))
                        {
                            row[colPos] = null;
                            
                        }
                        else
                        {
                            row[colPos] = Operations.Parse(mapping.TypeCode, mapping.Rank, value);    
                        }
                        
                        
//                        var result = _csvReader.TryGetField(mapping.DataType, mapping.Position, out object value);
//                        if (result)
//                        {
//                            row[colPos] = value; // Operations.Parse(mapping.TypeCode, mapping.Rank, value);
//                        }
//                        else
//                        {
//                            row[colPos] = null;
//                        }

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

                    if (SelectQuery == null || SelectQuery.EvaluateRowFilter(row, _table))
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