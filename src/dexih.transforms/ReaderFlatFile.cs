using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.IO;
using System.Threading;
using CsvHelper;
using System.Linq;
using dexih.functions.File;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    public class ReaderFlatFile : Transform
    {
        private bool _isOpen = false;

        private DexihFiles _files;

        private readonly FileHandlerBase _fileHandler;
        private object[] _baseRow;

        private readonly ConnectionFlatFile _fileConnection;

        private readonly int _fileNameOrdinal;

        private ICollection<Filter> _filters;

        private readonly bool _previewMode;

		public FlatFile CacheFlatFile => (FlatFile)CacheTable;

        public ReaderFlatFile(Connection connection, FlatFile table, bool previewMode)
        {
            ReferenceConnection = connection;
            _fileConnection = (ConnectionFlatFile)connection;
            CacheTable = table;

            _previewMode = previewMode;

            switch (table.FormatType)
            {
                    case DataType.ETypeCode.Json:
                        _fileHandler = new FileHandlerJson(table, table.RowPath);
                        break;
                    case DataType.ETypeCode.Text:
                        _fileHandler = new FileHandlerText(table, table.FileConfiguration);
                        break;
                    case DataType.ETypeCode.Xml:
                        _fileHandler = new FileHandlerXml(table, table.RowPath);
                        break;
                    default:
                        throw new ConnectionException(
                            $"The format type ${table.FormatType} is not currently supported.");
            }
            
            _fileNameOrdinal = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.FileName);
            _baseRow = new object[table.Columns.Count];
        }

        protected override void Dispose(bool disposing)
        {
            _fileHandler?.Dispose();

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
            
            if (_files.MoveNext() == false)
            {
                throw new ConnectionException($"There are no matching files in the incoming directory.");
            }

            _filters = query?.Filters;

            var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, EFlatFilePath.Incoming, _files.Current.FileName);
            if (_fileNameOrdinal >= 0)
            {
                _baseRow[_fileNameOrdinal] = _files.Current.FileName;
            }

            try
            {
                await _fileHandler.SetStream(fileStream, _filters);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to read the file {_files.Current.FileName}.  {ex.Message}", ex);
            }

            return true;

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
                object[] row;
                try
                {
                    row = await _fileHandler.GetRow(_baseRow);
                }
                catch (Exception ex)
                {
                    throw new ConnectionException("The flatfile reader failed with the following message: " + ex.Message, ex);
                }

                if (row == null)
                {
                    _fileHandler.Dispose();

                    // If we are managing files, then move the file after the read is finished.
                    // if this is preview mode, don't move files.
                    if (CacheFlatFile.AutoManageFiles && _previewMode == false)
                    {
                        try
                        {
                            await _fileConnection.MoveFile(CacheFlatFile, EFlatFilePath.Incoming, EFlatFilePath.Processed, _files.Current.FileName); //backup the completed file
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
                            if (_fileNameOrdinal >= 0)
                            {
                                _baseRow[_fileNameOrdinal] = _files.Current.FileName;
                            }
                            await _fileHandler.SetStream(fileStream, _filters);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Failed to read the file {_files.Current.FileName}.  {ex.Message}", ex);
                        }
                        
                        try
                        {
                            row = await _fileHandler.GetRow(_baseRow);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Flat file reader failed during the reading the file {_files.Current.FileName}.  {ex.Message}", ex);
                        }
                        if (row == null)
                        {
                            return await ReadRecord(cancellationToken); // this creates a recurive loop to cater for empty files.
                        }
                    }
                }

                return row;

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
