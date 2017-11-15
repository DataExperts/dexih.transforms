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
        public abstract Task<ReturnValue<List<string>>> GetFileShares(string serverName, string userName, string password);
        public abstract Task<ReturnValue> CreateDirectory(string rootDirectory, string subDirectory);
        public abstract Task<ReturnValue> MoveFile(string rootDirectory, string fromDirectory, string toDirectory, string fileName);
        public abstract Task<ReturnValue> DeleteFile(string rootDirectory, string subDirectory, string fileName);
        public abstract Task<ReturnValue<DexihFiles>> GetFileEnumerator(string mainDirectory, string subDirectory, string searchPattern);
        public abstract Task<ReturnValue<List<DexihFileProperties>>> GetFileList(string mainDirectory, string subDirectory);
        public abstract Task<ReturnValue<Stream>> GetReadFileStream(Table table, string subDirectory, string fileName);
        public abstract Task<ReturnValue<Stream>> GetWriteFileStream(Table table, string subDirectory, string fileName);
        public abstract Task<ReturnValue> SaveFileStream(Table table, string fileName, Stream fileStream);
        public abstract Task<ReturnValue> TestFileConnection();


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

		public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken)
        {
			var flatFile = (FlatFile)table;
            //create the subdirectories
            await CreateDirectory(flatFile, EFlatFilePath.Incoming);
        }

        public override async Task CreateDatabase(string databaseName, CancellationToken cancellationToken)
        {
            DefaultDatabase = databaseName;
            //create the subdirectories
            var returnValue = await CreateDirectory(null, EFlatFilePath.None);
            return;
        }

        public async Task<bool> CreateFilePaths(FlatFile flatFile)
        {
            Boolean returnValue;
            //create the subdirectories
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.Incoming);
            if (returnValue == false) return returnValue;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.Outgoing);
            if (returnValue == false) return returnValue;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.Processed);
            if (returnValue == false) return returnValue;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.Rejected);
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
            return await SaveFileStream(flatFile, EFlatFilePath.Incoming, fileName, fileStream);
        }

        public async Task<List<DexihFileProperties>> GetFiles(FlatFile flatFile, EFlatFilePath path)
        {
            return await GetFileList(flatFile, path);
        }

        public async Task<Stream> DownloadFiles(FlatFile flatFile, EFlatFilePath path, string[] fileNames, bool zipFiles = false)
        {
            if (zipFiles)
            {
                var memoryStream = new MemoryStream();

                using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Update, true))
                {
                    foreach (var fileName in fileNames)
                    {
                        var fileStreamResult = await GetReadFileStream(flatFile, path, fileName);
                        var fileEntry = archive.CreateEntry(fileName);

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
                var fileName = table.Name + DateTime.Now.ToString("_yyyyMMddHHmmss") + ".csv";
                var writerResult = await GetWriteFileStream(flatFile, EFlatFilePath.Outgoing, fileName);

                if(writerResult == null)
                {
                    throw new ConnectionException($"Flat file write failed, could not get a write stream for {flatFile.Name}.");
                }

                //open a new filestream and write a headerrow
                _fileStream = writerResult;
                _fileWriter = new StreamWriter(_fileStream);
                _csvWriter = new CsvWriter(_fileWriter);


                if (flatFile.FileConfiguration.HasHeaderRecord)
                {
                    var s = new string[table.Columns.Count];
                    for (var j = 0; j < table.Columns.Count; j++)
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

        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken)
        {
            try
            {
                while(await reader.ReadAsync(cancellationToken))
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new ConnectionException("Insert bulk operation cancelled.");
                    }

                    var s = new string[reader.FieldCount];
                    for (var j = 0; j < reader.FieldCount; j++)
                    {
                        _csvWriter.WriteField(reader[j]);
                    }
                    _csvWriter.NextRecord();
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Insert bulk operation failed.  {ex.Message}", ex);
            }
        }


        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken)
        {
            return await GetFileShares(Server, Username, Password);
        }

        public override Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken)
        {
            try
            {
                var flatFile = (FlatFile)originalTable;

                if (flatFile.FileConfiguration == null || flatFile.FileSample == null)
                {
                    return new ReturnValue<Table>(false, "The properties have not been set to import the flat files structure.  Required properties are (FileFormat)FileFormat and (Stream)FileSample.", null);
                }

                MemoryStream stream = new MemoryStream();
                StreamWriter writer = new StreamWriter(stream);
                writer.Write(flatFile.FileSample);
                writer.Flush();
                stream.Position = 0;

                string[] headers;
                if (flatFile.FileConfiguration.HasHeaderRecord)
                {
                    try
                    {
                        using (var csv = new CsvReader(new StreamReader(stream), flatFile.FileConfiguration))
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
                    using (var csv = new CsvReader(new StreamReader(stream), flatFile.FileConfiguration))
                    {
                        csv.Read();
                        headers = Enumerable.Range(0, csv.CurrentRecord.Count()).Select(c => "column-" + c.ToString().PadLeft(3, '0')).ToArray();
                    }
                }

                //The new datatable that will contain the table schema
                var newFlatFile = new FlatFile();
                flatFile.Name = originalTable.Name;
                newFlatFile.Columns.Clear();
                newFlatFile.LogicalName = newFlatFile.Name;
                newFlatFile.Description = "";
                newFlatFile.FileConfiguration = flatFile.FileConfiguration;

                TableColumn col;

                foreach (var field in headers)
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

                return new ReturnValue<Table>(true, newFlatFile);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to import the file structure. {ex.Message}", ex);
            }
        }

        public override Task<List<Table>> GetTableList(CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override async Task TruncateTable(Table table, CancellationToken cancellationToken)
        {
            var flatFile = (FlatFile)table;
            var fileEnumerator = await GetFileEnumerator(flatFile, EFlatFilePath.Incoming, flatFile.FileMatchPattern);
            if(fileEnumerator == null)
            {
                throw new ConnectionException($"Truncate failed, as no files were found.");
            }

            while(fileEnumerator.MoveNext())
            {
                var deleteResult = await DeleteFile(flatFile, EFlatFilePath.Incoming, fileEnumerator.Current.FileName);
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

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancellationToken)
        {
            try
            {
                var rows = 0;

				var fileName = table.Name + DateTime.Now.ToString("_yyyyMMddHHmmss") + ".csv";
                var flatFile = (FlatFile)table;

                //open a new filestream 
                using (var writer = await GetWriteFileStream(flatFile, EFlatFilePath.Outgoing, fileName))
                using (var streamWriter = new StreamWriter(writer))
                using (var csv = new CsvWriter(streamWriter, flatFile.FileConfiguration))
                {

                    if (!(queries?.Count >= 0))
                        return 0L;

                    if (flatFile.FileConfiguration.HasHeaderRecord)
                    {
                        //write a header row.
                        var s = new string[table.Columns.Count];
                        for (var j = 0; j < queries[0].InsertColumns.Count; j++)
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
                return 0L;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Failed to run execute insert on {table.Name}.  {ex.Message}.");
            }
        }


        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken)
        {
            var timer = Stopwatch.StartNew();

            var flatFile = (FlatFile)table;
            using (var reader = new ReaderFlatFile(this, flatFile, true))
            {
                var openResult = await reader.Open(0, query, cancellationToken);
                if (!openResult)
                {
                    throw new ConnectionException($"Failed to run execute scalar on {table.Name}.  The reader failed to open.");
                }

                var row = await reader.ReadAsync(cancellationToken);
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

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
			var flatFile = (FlatFile)table;
            var reader = new ReaderFlatFile(this, flatFile, previewMode);
            return reader;
        }


    }
}
