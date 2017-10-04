using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using dexih.functions;
using System.Data.Common;
using dexih.transforms;
using System.Threading;
using System.Diagnostics;
using static dexih.connections.flatfile.FlatFile;
using System.IO.Compression;
using CsvHelper;
using System.Linq;
using Dexih.Utils.CopyProperties;
using dexih.transforms.Exceptions;
using static Dexih.Utils.DataType.DataType;
using dexih.functions.Query;

namespace dexih.connections.flatfile
{
    public abstract class ConnectionFlatFile : Connection
    {
        public abstract Task<List<string>> GetFileShares(string serverName, string userName, string password);
        public abstract Task<bool> CreateDirectory(FlatFile file, EFlatFilePath path);
        public abstract Task<bool> MoveFile(FlatFile file, EFlatFilePath fromPath, EFlatFilePath toPath, string fileName);
        public abstract Task<bool> DeleteFile(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<DexihFiles> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern);
        public abstract Task<List<DexihFileProperties>> GetFileList(FlatFile file, EFlatFilePath path);
        public abstract Task<Stream> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<Stream> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<bool> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream fileStream);
        public abstract Task<bool> TestFileConnection();


        public override string ServerHelp => "Path for the files (use //server/path format)";
        public override string DefaultDatabaseHelp => "";
        public override bool AllowNtAuth => false;
        public override bool AllowUserPass => true;
        public override bool CanBulkLoad => true;
        public override bool CanSort => false;

        public override bool CanFilter => true;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanAggregate => false;
        public override bool CanUseBinary => false;
        public override bool CanUseSql => false;
        public override bool DynamicTableCreation => true;


        public override string DatabaseTypeName => "Flat Files";
        public override ECategory DatabaseCategory => ECategory.File;

        private Stream _fileStream;
        private StreamWriter _fileWriter;
        private CsvWriter _csvWriter;

        public string LastWrittenFile { get; protected set; } = "";

		public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
			var flatFile = (FlatFile)table;
            //create the subdirectories
            await CreateDirectory(flatFile, EFlatFilePath.incoming);
        }

        public override async Task CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            DefaultDatabase = databaseName;
            //create the subdirectories
            var returnValue = await CreateDirectory(null, EFlatFilePath.none);
            return;
        }

        public async Task<bool> CreateFilePaths(FlatFile flatFile)
        {
            Boolean returnValue;
            //create the subdirectories
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.incoming);
            if (returnValue == false) return returnValue;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.outgoing);
            if (returnValue == false) return returnValue;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.processed);
            if (returnValue == false) return returnValue;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.rejected);
            return returnValue;
        }

        /// <summary>
        /// Adds a guid to the file name and moves it to the Incoming directory.
        /// </summary>
        /// <param name="flatFile"></param>
        /// <param name="fileName"></param>
        /// <param name="fromDirectory"></param>
        /// <param name="toDirectory"></param>
        /// <returns></returns>
        public async Task<bool> MoveFile(FlatFile flatFile, string fileName, EFlatFilePath fromDirectory, EFlatFilePath toDirectory)
        {
            return await MoveFile(flatFile, fromDirectory, toDirectory, fileName);
        }

        public async Task<bool> SaveIncomingFile(FlatFile flatFile, string fileName, Stream fileStream)
        {
            return await SaveFileStream(flatFile, EFlatFilePath.incoming, fileName, fileStream);
        }

        public async Task<List<DexihFileProperties>> GetFiles(FlatFile flatFile, EFlatFilePath path)
        {
            return await GetFileList(flatFile, path);
        }

        public async Task<Stream> DownloadFiles(FlatFile flatFile, EFlatFilePath path, string[] fileNames, bool zipFiles = false)
        {
            if (zipFiles)
            {
                MemoryStream memoryStream = new MemoryStream();

                using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Update, true))
                {
                    foreach (var fileName in fileNames)
                    {
                        var fileStreamResult = await GetReadFileStream(flatFile, path, fileName);
                        ZipArchiveEntry fileEntry = archive.CreateEntry(fileName);

                        using (var fileEntryStream = fileEntry.Open())
                        {
                            fileStreamResult.CopyTo(fileEntryStream);
                        }
                    }
                }

                memoryStream.Seek(0, SeekOrigin.Begin);
                return memoryStream;
            } 
            else
            {
                if(fileNames.Length == 1)
                {
                    return await GetReadFileStream(flatFile, path, fileNames[0]);
                }
                else
                {
                    throw new ConnectionException("When downloading more than one file, the zip option must be set.");
                }
            }
        }

        public override async Task DataWriterStart(Table table)
        {
            try
            {
                var flatFile = (FlatFile)table;
                string fileName = table.Name + DateTime.Now.ToString("_yyyyMMddHHmmss") + ".csv";
                var writerResult = await GetWriteFileStream(flatFile, EFlatFilePath.outgoing, fileName);

                if(writerResult == null)
                {
                    throw new ConnectionException($"Flat file write failed, could not get a write stream for {flatFile.Name}.");
                }

                //open a new filestream and write a headerrow
                _fileStream = writerResult;
                _fileWriter = new StreamWriter(_fileStream);
                _csvWriter = new CsvWriter(_fileWriter);


                if (flatFile.FileFormat.HasHeaderRecord)
                {
                    string[] s = new string[table.Columns.Count];
                    for (Int32 j = 0; j < table.Columns.Count; j++)
                    {
                        _csvWriter.WriteField(table.Columns[j].Name);
                    }
                    _csvWriter.NextRecord();
                }

                LastWrittenFile = fileName;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Flat file write failed, could not get a write stream for {table.Name}.  {ex.Message}", ex);
            }
        }

        public override Task DataWriterFinish(Table table)
        {
            _fileWriter.Dispose();
            _fileStream.Dispose();
            _csvWriter.Dispose();

            return Task.CompletedTask;
        }

        public override async Task<long> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancelToken)
        {
            try
            {
                Stopwatch timer = Stopwatch.StartNew();

                while(await reader.ReadAsync(cancelToken))
                {
                    if (cancelToken.IsCancellationRequested)
                    {
                        throw new ConnectionException("Insert bulk operation cancelled.");
                    }

                    string[] s = new string[reader.FieldCount];
                    for (int j = 0; j < reader.FieldCount; j++)
                    {
                        _csvWriter.WriteField(reader[j]);
                    }
                    _csvWriter.NextRecord();
                }
                return timer.ElapsedTicks;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Insert bulk operation failed.  {ex.Message}", ex);
            }
        }


        public override async Task<List<string>> GetDatabaseList(CancellationToken cancelToken)
        {
            return await GetFileShares(Server, Username, Password);
        }

        public override Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancelToken)
        {
            try
            {
                var flatFile = (FlatFile)originalTable;

                if (flatFile.FileFormat == null || flatFile.FileSample == null)
                {
                    throw new ConnectionException($"The properties have not been set to import the flat files structure.  Required properties are (FileFormat)FileFormat and (Stream)FileSample.");
                }

                MemoryStream stream = new MemoryStream();
                StreamWriter writer = new StreamWriter(stream);
                writer.Write(flatFile.FileSample);
                writer.Flush();
                stream.Position = 0;

                string[] headers;
                if (flatFile.FileFormat.HasHeaderRecord)
                {
                    try
                    {
                        using (CsvReader csv = new CsvReader(new StreamReader(stream), flatFile.FileFormat))
                        {
                            csv.ReadHeader();
                            headers = csv.FieldHeaders;
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"Error occurred opening the filestream: {ex.Message}", ex);
                    }
                }
                else
                {
                    // if no header row specified, then just create a series column names "column001, column002 ..."
                    using (CsvReader csv = new CsvReader(new StreamReader(stream), flatFile.FileFormat))
                    {
                        csv.Read();
                        headers = Enumerable.Range(0, csv.CurrentRecord.Count()).Select(c => "column-" + c.ToString().PadLeft(3, '0')).ToArray();
                    }
                }

                //The new datatable that will contain the table schema
                FlatFile newFlatFile = new FlatFile();
                flatFile.Name = originalTable.Name;
                newFlatFile.Columns.Clear();
                newFlatFile.LogicalName = newFlatFile.Name;
                newFlatFile.Description = "";
                newFlatFile.FileFormat = flatFile.FileFormat;

                TableColumn col;

                foreach (string field in headers)
                {
                    col = new TableColumn()
                    {

                        //add the basic properties
                        Name = field,
                        LogicalName = field,
                        IsInput = false,
                        Datatype = ETypeCode.String,
                        DeltaType = TableColumn.EDeltaType.TrackingField,
                        Description = "",
                        AllowDbNull = true,
                        IsUnique = false
                    };
                    newFlatFile.Columns.Add(col);
                }

                col = new TableColumn()
                {

                    //add the basic properties
                    Name = "FileName",
                    LogicalName = "FileName",
                    IsInput = false,
                    Datatype = ETypeCode.String,
                    DeltaType = TableColumn.EDeltaType.FileName,
                    Description = "The name of the file the record was loaded from.",
                    AllowDbNull = false,
                    IsUnique = false
                };
                newFlatFile.Columns.Add(col);

                col = new TableColumn()
                {

                    //add the basic properties
                    Name = "FileRow",
                    LogicalName = "FileRow",
                    IsInput = false,
                    Datatype = ETypeCode.Int32,
                    DeltaType = TableColumn.EDeltaType.FileRowNumber,
                    Description = "The file row number the record came from.",
                    AllowDbNull = false,
                    IsUnique = false
                };
                newFlatFile.Columns.Add(col);

                return Task.FromResult<Table>(newFlatFile);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to import the file structure. {ex.Message}", ex);
            }
        }

        public override Task<List<Table>> GetTableList(CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task TruncateTable(Table table, CancellationToken cancelToken)
        {
            var flatFile = (FlatFile)table;
            var fileEnumerator = await GetFileEnumerator(flatFile, FlatFile.EFlatFilePath.incoming, flatFile.FileMatchPattern);
            if(fileEnumerator == null)
            {
                throw new ConnectionException($"Truncate failed, as no files were found.");
            }

            while(fileEnumerator.MoveNext())
            {
                var deleteResult = await DeleteFile(flatFile, FlatFile.EFlatFilePath.incoming, fileEnumerator.Current.FileName);
                if(!deleteResult)
                {
                    return;
                }
            }

            return;
        }

        public override async Task<Table> InitializeTable(Table table, int position)
        {
			var flatFile = new FlatFile();
            table.CopyProperties(flatFile, false);
            
			//use the default paths.
			flatFile.UseCustomFilePaths = false;
            flatFile.AutoManageFiles = true;
            
            await CreateFilePaths(flatFile);

            return flatFile;
        }

        public override Task<long> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<long> ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<Tuple<long, long>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            try
            {
                int rows = 0;
                Stopwatch timer = Stopwatch.StartNew();

                string fileName = table.Name + DateTime.Now.ToString("_yyyyMMddHHmmss") + ".csv";
                var flatFile = (FlatFile)table;

                //open a new filestream 
                using (var writer = await GetWriteFileStream(flatFile, EFlatFilePath.outgoing, fileName))
                using (var streamWriter = new StreamWriter(writer))
                using (var csv = new CsvWriter(streamWriter, flatFile.FileFormat))
                {

                    if (!(queries?.Count >= 0))
                        return Tuple.Create(timer.Elapsed.Ticks, (long)0);

                    if (flatFile.FileFormat.HasHeaderRecord)
                    {
                        //write a header row.
                        string[] s = new string[table.Columns.Count];
                        for (Int32 j = 0; j < queries[0].InsertColumns.Count; j++)
                        {
                            csv.WriteField(queries[0].InsertColumns[j].Column.Name);
                        }
                        csv.NextRecord();
                    }

                    foreach (var query in queries)
                    {
                        for (var j = 0; j < query.InsertColumns.Count; j++)
                        {
                            csv.WriteField(query.InsertColumns[j].Value);
                        }
                        csv.NextRecord();
                        rows++;
                    }
                }

                LastWrittenFile = fileName;
                timer.Stop();
                return Tuple.Create(timer.Elapsed.Ticks, (long)0); //sometimes reader returns -1, when we want this to be error condition.
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Failed to run execute insert on {table.Name}.  {ex.Message}.");
            }
        }


        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            var timer = Stopwatch.StartNew();

            FlatFile flatFile = (FlatFile)table;
            using (var reader = new ReaderFlatFile(this, flatFile, true))
            {
                var openResult = await reader.Open(0, query, cancelToken);
                if (!openResult)
                {
                    throw new ConnectionException($"Failed to run execute scalar on {table.Name}.  The reader failed to open.");
                }

                var row = await reader.ReadAsync(cancelToken);
                if (row)
                {
                    var value = reader[query.Columns[0].Column.Name];
                    return value;
                }
                else
                {
                    return null;
                }
            }
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
			FlatFile flatFile = (FlatFile)table;
            var reader = new ReaderFlatFile(this, flatFile, previewMode);
            return reader;
        }


    }
}
