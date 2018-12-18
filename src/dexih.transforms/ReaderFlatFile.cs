using System;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.File;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    public class ReaderFlatFile : Transform
    {
        private bool _isOpen = false;

        private DexihFiles _files;

        private readonly FileHandlerBase _fileHandler;

        private readonly ConnectionFlatFile _fileConnection;

        private readonly int _fileNameOrdinal;

        private SelectQuery _selectQuery;

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
                            $"The format type {table.FormatType} is not currently supported.");
            }
            
            _fileNameOrdinal = table.GetDeltaColumnOrdinal(TableColumn.EDeltaType.FileName);
        }
        
        public override void Close()
        {
            _fileHandler?.Dispose();
            _isOpen = false;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            _selectQuery = query;

            if (_isOpen)
            {
                throw new ConnectionException("The file reader connection is already open.");
            }
            
            // if a filename was specified in the query, use this, otherwise, get a list of files from the incoming directory.
            if (string.IsNullOrEmpty(query?.FileName))
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

            var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, EFlatFilePath.Incoming, _files.Current.FileName);

            try
            {
                await _fileHandler.SetStream(fileStream, query);
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
                    row = await _fileHandler.GetRow();
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
                            await _fileHandler.SetStream(fileStream, _selectQuery);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Failed to read the file {_files.Current.FileName}.  {ex.Message}", ex);
                        }
                        
                        try
                        {
                            row = await _fileHandler.GetRow();

                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Flat file reader failed during the reading the file {_files.Current.FileName}.  {ex.Message}", ex);
                        }
                        if (row == null)
                        {
                            return await ReadRecord(cancellationToken); // this creates a recursive loop to cater for empty files.
                        }
                    }
                }
                
                if (row != null && _fileNameOrdinal >= 0)
                {
                    row[_fileNameOrdinal] = _files.Current.FileName;
                }

                return row;

            }

        }
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            Reset();
            return Open(auditKey, query, cancellationToken);
        }

        public override bool FinalizeLookup()
        {
            Close();
            return true;
        }


    }
}
