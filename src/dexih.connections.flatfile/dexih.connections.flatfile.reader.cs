using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.IO;
using System.Threading;

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

        private int _currentFileRowNumber;

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

			_fileFormat = CacheFlatFile.FileFormat;

            _csvReader = new CsvReader(new StreamReader(fileStream.Value), _fileFormat);

            return new ReturnValue(true);
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
                return new ReturnValue(false, "The flatfile reader can not be reset", null);

        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            bool notfinished;
            try
            {
                notfinished = await _csvReader.ReadAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                throw new Exception("The flatfile reader failed with the following message: " + ex.Message, ex);
            }

            if (notfinished == false)
            {
                _csvReader.CloseFile();

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

                    _csvReader = new CsvReader(new StreamReader(fileStream.Value), _fileFormat);
                    try
                    {
                        notfinished = await _csvReader.ReadAsync(cancellationToken);
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
                return new ReturnValue<object[]>(false, null);

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
