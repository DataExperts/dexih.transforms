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

        private int _fileNameOrdinal;
        private int _fileRowNumberOrdinal;

        private int _currentFileRowNumber;

		public FlatFile CacheFlatFile => (FlatFile)CacheTable;

        public ReaderFlatFile(Connection connection, FlatFile table, bool previewMode)
        {
            ReferenceConnection = connection;
            _fileConnection = (ConnectionFlatFile)connection;
            CacheTable = table;

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

            var fileEnumerator = await _fileConnection.GetFileEnumerator(CacheFlatFile.FileRootPath, CacheFlatFile.AutoManageFiles ? CacheFlatFile.FileIncomingPath : "", CacheFlatFile.FileMatchPattern);
            if (fileEnumerator.Success == false)
                return fileEnumerator;

            _files = fileEnumerator.Value;
            _currentFileRowNumber = 0;

            if (_files.MoveNext() == false)
            {
                return new ReturnValue(false, $"There are no matching files in the incoming directory.", null);
            }

            var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, CacheFlatFile.AutoManageFiles ? CacheFlatFile.FileIncomingPath : "", _files.Current.FileName);
            if (fileStream.Success == false)
            {
                return fileStream;
            }

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

                // If we are managing files, then move the file after the read is finished.
                if (CacheFlatFile.AutoManageFiles)
                {
                    var moveFileResult = await _fileConnection.MoveFile(CacheFlatFile, _files.Current.FileName, CacheFlatFile.FileIncomingPath, CacheFlatFile.FileProcessedPath); //backup the completed file

                    if (!moveFileResult.Success)
                    {
                        return new ReturnValue<object[]>(moveFileResult);
                    }
                }

                if (moreRecords)
                {
                    var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, CacheFlatFile.AutoManageFiles ? CacheFlatFile.FileIncomingPath : "", _files.Current.FileName);
                    if (!fileStream.Success)
                    {
                        return new ReturnValue<object[]>(fileStream);
                    }

                    _currentFileRowNumber = 0;

                    // check if a row should be filtered.  If not return the value.
                    if (EvaluateRowFilter(row))
                    {
                        return row;
                    }
                    else
                    {
                        return new ReturnValue<object[]>(false, "The flatfile reader failed with the following message: " + ex.Message, ex);
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
                object[] row = new object[CacheTable.Columns.Count];
                _currentFileRowNumber++;

                _csvReader.GetValues(row);

                if(_fileNameOrdinal >= 0)
                {
                    row[_fileNameOrdinal] = _files.Current.FileName;
                }
                if (_fileRowNumberOrdinal >= 0)
                {
                    row[_fileRowNumberOrdinal] = _currentFileRowNumber;
                }

                return new ReturnValue<object[]>(true, row);
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
        public override Task<IEnumerable<object[]>> LookupRowDirect(List<Filter> filters, EDuplicateStrategy duplicateStrategy, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("Direct lookup not supported with flat files.");
        }
    }
}
