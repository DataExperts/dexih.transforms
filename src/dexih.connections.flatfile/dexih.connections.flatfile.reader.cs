using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.IO;
using System.Threading;
using CsvHelper;
using System.Linq;

namespace dexih.connections.flatfile
{
    public class ReaderFlatFile : Transform
    {
        private bool _isOpen = false;

        private DexihFiles _files;
        private CsvReader _csvReader;

        private FileFormat _fileFormat;

        private ConnectionFlatFile _fileConnection;

        private int _fileNameOrdinal;
        private int _fileRowNumberOrdinal;

        private Dictionary<int, (int position, Type dataType)> _csvOrdinalMappings;

        private int _currentFileRowNumber;

        private SelectQuery _query;

        private bool _previewMode;

		public FlatFile CacheFlatFile => (FlatFile)CacheTable;

        public ReaderFlatFile(Connection connection, FlatFile table, bool previewMode)
        {
            ReferenceConnection = connection;
            _fileConnection = (ConnectionFlatFile)connection;
            CacheTable = table;

            _previewMode = previewMode;
            _fileNameOrdinal = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.FileName);
            _fileRowNumberOrdinal = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.FileRowNumber);
        }

        protected override void Dispose(bool disposing)
        {
            _csvReader?.Dispose();

            _isOpen = false;

            base.Dispose(disposing);
        }

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;

            if (_isOpen)
            {
                return new ReturnValue(false, "The file reader connection is already open.", null);
            }

            var fileEnumerator = await _fileConnection.GetFileEnumerator(CacheFlatFile, FlatFile.EFlatFilePath.incoming, CacheFlatFile.FileMatchPattern);
            if (fileEnumerator.Success == false)
                return fileEnumerator;

            _files = fileEnumerator.Value;
            _currentFileRowNumber = 0;

            if (_files.MoveNext() == false)
            {
                return new ReturnValue(false, $"There are no matching files in the incoming directory.", null);
            }

            var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, FlatFile.EFlatFilePath.incoming, _files.Current.FileName);
            if (fileStream.Success == false)
            {
                return fileStream;
            }

            _query = query;

            _fileFormat = CacheFlatFile.FileFormat;

            InitializeCsvReader(new StreamReader(fileStream.Value));

            return new ReturnValue(true);

        }

        private void InitializeCsvReader(StreamReader streamReader)
        {
            _csvReader = new CsvReader(streamReader, _fileFormat);
            _csvReader.ReadHeader();

            _csvOrdinalMappings = new Dictionary<int, (int position, Type dataType)>();

            // create mappings from column name positions, to the csv field name positions.
            if(_fileFormat.MatchHeaderRecord || _fileFormat.HasHeaderRecord == false)
            {
                for(var col = 0; col < CacheTable.Columns.Count; col++)
                {
                    var column = CacheTable.Columns[col];
                    if (column.DeltaType != TableColumn.EDeltaType.FileName && column.DeltaType != TableColumn.EDeltaType.FileRowNumber)
                    {
                        for (var csvPos = 0; csvPos < _csvReader.FieldHeaders.Length; csvPos++)
                        {
                            if (_csvReader.FieldHeaders[csvPos] == column.Name)
                            {
                                _csvOrdinalMappings.Add(col, (csvPos, DataType.GetType(column.Datatype)));
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                // not matching in header, just create mappings equal
                for (var col = 0; col < CacheTable.Columns.Count; col++)
                {
                    var column = CacheTable.Columns[col];
                    if (column.DeltaType != TableColumn.EDeltaType.FileName && column.DeltaType != TableColumn.EDeltaType.FileRowNumber)
                    {
                        _csvOrdinalMappings.Add(col, (col, DataType.GetType(column.Datatype)));
                    }
                }
            }
        }

        public override string Details()
        {
            return "FlatFile";
        }

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override ReturnValue ResetTransform()
        {
            if (_isOpen)
            {
                return new ReturnValue(true);
            }
            else
            {
                return new ReturnValue(false, "The flatfile reader can not be reset", null);
            }
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            while (true)
            {
                bool notfinished;
                try
                {
                    notfinished = _csvReader.Read();
                }
                catch (Exception ex)
                {
                    throw new Exception("The flatfile reader failed with the following message: " + ex.Message, ex);
                }

                if (notfinished == false)
                {
                    _csvReader.Dispose();

                    // If we are managing files, then move the file after the read is finished.
                    // if this is preview mode, don't move files.
                    if (CacheFlatFile.AutoManageFiles && _previewMode == false)
                    {
                        var moveFileResult = await _fileConnection.MoveFile(CacheFlatFile, FlatFile.EFlatFilePath.incoming, FlatFile.EFlatFilePath.processed, _files.Current.FileName); //backup the completed file

                        if (!moveFileResult.Success)
                        {
                            return new ReturnValue<object[]>(moveFileResult);
                        }
                    }

                    if (_files.MoveNext() == false)
                        _isOpen = false;
                    else
                    {
                        var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, FlatFile.EFlatFilePath.incoming, _files.Current.FileName);
                        if (!fileStream.Success)
                        {
                            return new ReturnValue<object[]>(fileStream);
                        }

                        _currentFileRowNumber = 0;

                        InitializeCsvReader(new StreamReader(fileStream.Value));
                        try
                        {
                            notfinished = _csvReader.Read();
                        }
                        catch (Exception ex)
                        {
                            return new ReturnValue<object[]>(false, "The flatfile reader failed with the following message: " + ex.Message, ex);
                        }
                        if (notfinished == false)
                            return await ReadRecord(cancellationToken); // this creates a recurive loop to cater for empty files.
                    }
                }

                if (notfinished)
                {
                    object[] row = new object[CacheTable.Columns.Count];
                    _currentFileRowNumber++;

                    foreach(var colPos in _csvOrdinalMappings.Keys)
                    {
                        var mapping = _csvOrdinalMappings[colPos];
                        row[colPos] = _csvReader.GetField(mapping.dataType, mapping.position);
                    }

                    if(_fileNameOrdinal >= 0)
                    {
                        row[_fileNameOrdinal] = _files.Current.FileName;
                    }

                    if(_fileRowNumberOrdinal >= 0)
                    {
                        row[_fileRowNumberOrdinal] = _currentFileRowNumber;
                    }

                    // check if a row should be filtered.  If not return the value.
                    if (EvaluateRowFilter(row))
                    {
                        return new ReturnValue<object[]>(true, row);
                    }
                    else
                    {
                        continue; //continue loop until an unfiltered row is found.
                    }
                }
                else
                    return new ReturnValue<object[]>(false, null);
            }

        }

        /// <summary>
        /// Tests is a row should be filtered based on the filters provided.  
        /// </summary>
        /// <param name="row"></param>
        /// <param name="headerOrdinals"></param>
        /// <param name="filters"></param>
        /// <returns>true = don't filter, false = filtered</returns>
        private bool EvaluateRowFilter(object[] row)
        {
            if (_query != null && _query.Filters?.Count > 0)
            {
                var filters = _query.Filters;
                var filterResult = true;
                var isFirst = true;

                foreach (var filter in filters)
                {
                    var column1Value = filter.Column1 == null
                        ? null
                        : row[CacheFlatFile.GetOrdinal(filter.Column1.Name)];
                    var column2Value = filter.Column2 == null
                        ? null
                        : row[CacheFlatFile.GetOrdinal(filter.Column2.Name)];

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
            else
            {
                return true;
            }
        }

        public override bool CanLookupRowDirect { get; } = false;

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public override Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters, CancellationToken cancelToken)
        {
            throw new NotSupportedException("Direct lookup not supported with flat files.");
        }
    }
}
