using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.IO;
using System.Threading;
using CsvHelper;
using System.Linq;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;

namespace dexih.connections.flatfile
{
    public class ReaderFlatFile : Transform
    {
        private bool _isOpen = false;

        private DexihFiles _files;
        private CsvReader _csvReader;

        private FileConfiguration _fileConfiguration;

        private readonly ConnectionFlatFile _fileConnection;

        private readonly int _fileNameOrdinal;
        private readonly int _fileRowNumberOrdinal;

        private Dictionary<int, (int position, Type dataType)> _csvOrdinalMappings;

        private int _currentFileRowNumber;

        private SelectQuery _query;

        private readonly bool _previewMode;

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

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            if (_isOpen)
            {
                throw new ConnectionException("The file reader connection is already open.");
            }

            // if a filename was specified in the query, use this, otherwise, get a list of files from the incoming directory.
            if (query == null || string.IsNullOrEmpty(query.FileName))
            {
                _files = await _fileConnection.GetFileEnumerator(CacheFlatFile, EFlatFilePath.Incoming,
                    CacheFlatFile.FileMatchPattern);
            }
            else
            {
                _files = await _fileConnection.GetFileEnumerator(CacheFlatFile, query.Path,
                    query.FileName);
            }
            
            _currentFileRowNumber = 0;

            if (_files.MoveNext() == false)
            {
                throw new ConnectionException($"There are no matching files in the incoming directory.");
            }

            _query = query;
            _fileConfiguration = CacheFlatFile.FileConfiguration;

            var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, EFlatFilePath.Incoming, _files.Current.FileName);
            InitializeCsvReader(new StreamReader(fileStream));

            return true;

        }

        private void InitializeCsvReader(StreamReader streamReader)
        {
			if (_fileConfiguration != null)
			{
				_csvReader = new CsvReader(streamReader, _fileConfiguration);
			} 
			else 
			{
				_csvReader = new CsvReader(streamReader);
			}
				

            _csvOrdinalMappings = new Dictionary<int, (int position, Type dataType)>();

            // create mappings from column name positions, to the csv field name positions.
			if(_fileConfiguration != null && ( _fileConfiguration.MatchHeaderRecord || !_fileConfiguration.HasHeaderRecord))
            {
				_csvReader.ReadHeader();

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

        public override bool ResetTransform()
        {
            if (_isOpen)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            while (true)
            {
                bool moreRecords;
                try
                {
                    moreRecords = _csvReader.Read();
                }
                catch (Exception ex)
                {
                    throw new ConnectionException("The flatfile reader failed with the following message: " + ex.Message, ex);
                }

                if (!moreRecords)
                {
                    _csvReader.Dispose();

                    // If we are managing files, then move the file after the read is finished.
                    // if this is preview mode, don't move files.
                    if (CacheFlatFile.AutoManageFiles && _previewMode == false)
                    {
                        try
                        {
                            var moveFileResult = await _fileConnection.MoveFile(CacheFlatFile, EFlatFilePath.Incoming, EFlatFilePath.Processed, _files.Current.FileName); //backup the completed file
                        }
                        catch(Exception ex)
                        {
                            throw new ConnectionException($"Failed to move the file {_files.Current.FileName} from the incoming to processed directory.  {ex.Message}", ex);
                        }
                    }

                    if (_files.MoveNext() == false)
                        _isOpen = false;
                    else
                    {
                        try
                        {
                            var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, EFlatFilePath.Incoming, _files.Current.FileName);
                            _currentFileRowNumber = 0;
                            InitializeCsvReader(new StreamReader(fileStream));
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Failed to read the file {_files.Current.FileName}.  {ex.Message}", ex);
                        }

                        
                        try
                        {
                            moreRecords = _csvReader.Read();
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Flat file reader failed during the reading the file {_files.Current.FileName}.  {ex.Message}", ex);
                        }
                        if (!moreRecords)
                        {
                            return await ReadRecord(cancellationToken); // this creates a recurive loop to cater for empty files.
                        }
                    }
                }

                if (moreRecords)
                {
                    var row = new object[CacheTable.Columns.Count];
                    _currentFileRowNumber++;

                    foreach (var colPos in _csvOrdinalMappings.Keys)
                    {
                        var mapping = _csvOrdinalMappings[colPos];
                        row[colPos] = _csvReader.GetField(mapping.dataType, mapping.position);
                    }

                    if (_fileNameOrdinal >= 0)
                    {
                        row[_fileNameOrdinal] = _files.Current.FileName;
                    }

                    if (_fileRowNumberOrdinal >= 0)
                    {
                        row[_fileRowNumberOrdinal] = _currentFileRowNumber;
                    }

                    // check if a row should be filtered.  If not return the value.
                    if (EvaluateRowFilter(row))
                    {
                        return row;
                    }
                    else
                    {
                        continue; //continue loop until an unfiltered row is found.
                    }
                }
                else
                {
                    return null;
                }
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
        /// <param name="duplicateStrategy"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override Task<ICollection<object[]>> LookupRowDirect(List<Filter> filters, EDuplicateStrategy duplicateStrategy, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("Direct lookup not supported with flat files.");
        }
    }
}
