using System;
using System.Collections.Generic;
using System.Linq;
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
        private IAsyncEnumerator<FileProperties> _files;

        private readonly FileHandlerBase _fileHandler;

        private readonly ConnectionFlatFile _fileConnection;

        private readonly int _fileNameOrdinal;
        private readonly int _fileDateOrdinal;

        private readonly string _fileNameColumn;
        private readonly string _fileDateColumn;

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
                    case ETypeCode.Json:
                        _fileHandler = new FileHandlerJson(table, table.RowPath);
                        break;
                    case ETypeCode.Text:
                        _fileHandler = new FileHandlerText(table, table.FileConfiguration);
                        break;
                    case ETypeCode.Xml:
                        _fileHandler = new FileHandlerXml(table, table.RowPath);
                        break;
                    default:
                        throw new ConnectionException(
                            $"The format type {table.FormatType} is not currently supported.");
            }
            _fileNameOrdinal = table.GetOrdinal(EDeltaType.FileName);
            _fileDateOrdinal = table.GetOrdinal(EDeltaType.FileDate);

            if (_fileNameOrdinal >= 0)
            {
                _fileNameColumn = table[_fileNameOrdinal].Name;
            }

            if (_fileDateOrdinal >= 0)
            {
                _fileDateColumn = table[_fileDateOrdinal].Name;
            }
        }
        
        public override string TransformName { get; } = "Flat File Reader";

        public override Dictionary<string, object> TransformProperties()
        {
            return new Dictionary<string, object>()
            {
                {"FileType", _fileHandler?.FileType??"Unknown"},
            };
        }
        
        protected override void CloseConnections()
        {
            _fileHandler?.Dispose();
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            SelectQuery = requestQuery;

            if (IsOpen)
            {
                throw new ConnectionException("The file reader connection is already open.");
            }

            IsOpen = true;
            
            // if a filename was specified in the query, use this, otherwise, get a list of files from the incoming directory.
            if (string.IsNullOrEmpty(requestQuery?.FileName))
            {
                _files = _fileConnection.GetFileEnumerator(CacheFlatFile, EFlatFilePath.Incoming,
                    CacheFlatFile.FileMatchPattern, cancellationToken).GetAsyncEnumerator(cancellationToken);
            }
            else
            {
                _files = _fileConnection.GetFileEnumerator(CacheFlatFile, requestQuery.Path,
                    requestQuery.FileName, cancellationToken).GetAsyncEnumerator(cancellationToken);
            }

            var file = await GetNextFile(requestQuery);
            
            if (file == null)
            {
                throw new ConnectionException($"There are no matching files in the incoming directory.");
            }

            if (_fileDateOrdinal >= 0)
            {
                await file.LoadAttributes();
            }

            var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, EFlatFilePath.Incoming, file.FileName, cancellationToken);

            try
            {
                await _fileHandler.SetStream(fileStream, requestQuery);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to read the file {_files.Current.FileName}.  {ex.Message}", ex);
            }
            
            // if the flatfile is already sorted, then update the generated query to indicate this.
            if (CacheFlatFile.OutputSortFields?.Count > 0)
            {
                GeneratedQuery = new SelectQuery()
                {
                    Sorts = CacheFlatFile.OutputSortFields
                };
            }

            return true;

        }

        /// <summary>
        /// Evaluates filters on the file name and file date to save loading each file
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        private async Task<FileProperties> GetNextFile(SelectQuery query)
        {
            while (await _files.MoveNextAsync())
            {
                var file = _files.Current;

                if (query?.Filters == null || query.Filters.Count == 0)
                {
                    return file;
                }

                foreach (var filter in query.Filters)
                {
                    if (!string.IsNullOrEmpty(_fileNameColumn))
                    {
                        if (filter.Column1?.Name == _fileNameColumn && filter.Column2 == null)
                        {
                            if (!filter.Evaluate(file.FileName, filter.Value2)) continue;
                        }

                        if (filter.Column2?.Name == _fileNameColumn && filter.Column1 == null)
                        {
                            if (!filter.Evaluate(file.FileName, filter.Value1)) continue;
                        }
                    }

                    if (!string.IsNullOrEmpty(_fileDateColumn))
                    {
                        await file.LoadAttributes();
                        if (filter.Column1?.Name == _fileDateColumn && filter.Column2 == null)
                        {
                            if (!filter.Evaluate(file.LastModified, filter.Value2)) continue;
                        }

                        if (filter.Column2?.Name == _fileDateColumn && filter.Column1 == null)
                        {
                            if (!filter.Evaluate(file.LastModified, filter.Value1)) continue;
                        }
                    }

                    return file;
                }
            }

            return null;
        }

        public override bool ResetTransform() => IsOpen;

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                object[] row;
                try
                {
                    row = await _fileHandler.GetRow(_files.Current);
                }
                catch (Exception ex)
                {
                    throw new ConnectionException($"The flat file reader failed to read the file {_files.Current.FileName} due to the following error: " + ex.Message, ex);
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
                            await _fileConnection.MoveFile(CacheFlatFile, EFlatFilePath.Incoming, EFlatFilePath.Processed, _files.Current.FileName, cancellationToken); //backup the completed file
                        }
                        catch(Exception ex)
                        {
                            throw new ConnectionException($"Failed to move the file {_files.Current.FileName} from the incoming to processed directory.  {ex.Message}", ex);
                        }
                    }

                    var file = await GetNextFile(SelectQuery);
                    if (file == null)
                    {
                        return null;
                    }
                    
                    if (_fileDateOrdinal >= 0)
                    {
                        await file.LoadAttributes();
                    }


                    try
                    {
                        var fileStream = await _fileConnection.GetReadFileStream(CacheFlatFile, EFlatFilePath.Incoming, file.FileName, cancellationToken);
                        await _fileHandler.SetStream(fileStream, SelectQuery);
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"Failed to read the file {file.FileName}.  {ex.Message}", ex);
                    }
                        
                    try
                    {
                        row = await _fileHandler.GetRow(_files.Current);

                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"Flat file reader failed during the reading the file {file.FileName}.  {ex.Message}", ex);
                    }
                    if (row == null)
                    {
                        return await ReadRecord(cancellationToken); // this creates a recursive loop to cater for empty files.
                    }
                }
                
                // if (_fileNameOrdinal >= 0)
                // {
                //     row[_fileNameOrdinal] = _files.Current.FileName;
                // }
                //
                // if (_fileDateOrdinal >= 0)
                // {
                //     row[_fileDateOrdinal] = _files.Current.LastModified;
                // }

                return row;

            }

        }
        
        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            Reset();
            Dispose();
            return Open(auditKey, query, cancellationToken);
        }

        public override bool FinalizeLookup()
        {
            Close();
            return true;
        }


    }
}
