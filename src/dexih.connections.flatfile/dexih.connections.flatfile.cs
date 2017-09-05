using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using dexih.functions;
using System.Data.Common;
using static dexih.functions.DataType;
using dexih.transforms;
using System.Threading;
using System.Diagnostics;
using static dexih.connections.flatfile.FlatFile;
using System.IO.Compression;
using CsvHelper;
using System.Linq;

namespace dexih.connections.flatfile
{
    public abstract class ConnectionFlatFile : Connection
    {
        public abstract Task<ReturnValue<List<string>>> GetFileShares(string serverName, string userName, string password);
        public abstract Task<ReturnValue> CreateDirectory(FlatFile file, EFlatFilePath path);
        public abstract Task<ReturnValue> MoveFile(FlatFile file, EFlatFilePath fromPath, EFlatFilePath toPath, string fileName);
        public abstract Task<ReturnValue> DeleteFile(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<ReturnValue<DexihFiles>> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern);
        public abstract Task<ReturnValue<List<DexihFileProperties>>> GetFileList(FlatFile file, EFlatFilePath path);
        public abstract Task<ReturnValue<Stream>> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<ReturnValue<Stream>> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<ReturnValue> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream fileStream);
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

		public override async Task<ReturnValue> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
			var flatFile = (FlatFile)table;
            //create the subdirectories
            return await CreateDirectory(flatFile, EFlatFilePath.incoming);
        }

        public override async Task<ReturnValue> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            ReturnValue returnValue;
            DefaultDatabase = databaseName;
            //create the subdirectories
            returnValue = await CreateDirectory(null, EFlatFilePath.none);
            return returnValue;
        }

        public async Task<ReturnValue> CreateFilePaths(FlatFile flatFile)
        {
            ReturnValue returnValue;
            //create the subdirectories
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.incoming);
            if (returnValue.Success == false) return returnValue;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.processed);
            if (returnValue.Success == false) return returnValue;
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
        public async Task<ReturnValue> MoveFile(FlatFile flatFile, string fileName, EFlatFilePath fromDirectory, EFlatFilePath toDirectory)
        {
            return await MoveFile(flatFile, fromDirectory, toDirectory, fileName);
        }

        public async Task<ReturnValue> SaveIncomingFile(FlatFile flatFile, string fileName, Stream fileStream)
        {
            return await SaveFileStream(flatFile, EFlatFilePath.incoming, fileName, fileStream);
        }

        public async Task<ReturnValue<List<DexihFileProperties>>> GetFiles(FlatFile flatFile, EFlatFilePath path)
        {
            return await GetFileList(flatFile, path);
        }

        public async Task<ReturnValue<Stream>> DownloadFiles(FlatFile flatFile, EFlatFilePath path, string[] fileNames, bool zipFiles = false)
        {
            if (zipFiles)
            {
                MemoryStream memoryStream = new MemoryStream();

                using (var archive = new ZipArchive(memoryStream, ZipArchiveMode.Update, true))
                {
                    foreach (var fileName in fileNames)
                    {
                        var fileStreamResult = await GetReadFileStream(flatFile, path, fileName);
                        if (!fileStreamResult.Success)
                        {
                            return new ReturnValue<Stream>(fileStreamResult);
                        }
                        ZipArchiveEntry fileEntry = archive.CreateEntry(fileName);

                        using (var fileEntryStream = fileEntry.Open())
                        {
                            fileStreamResult.Value.CopyTo(fileEntryStream);
                        }
                    }
                }

                memoryStream.Seek(0, SeekOrigin.Begin);
                return new ReturnValue<Stream>(true, memoryStream);
            } 
            else
            {
                if(fileNames.Length == 1)
                {
                    return await GetReadFileStream(flatFile, path, fileNames[0]);
                }
                else
                {
                    return new ReturnValue<Stream>(false, "When downloading more than one file, the zip option must be set.", null);
                }
            }
        }

        public override async Task<ReturnValue> DataWriterStart(Table table)
        {
            try
            {
                var flatFile = (FlatFile)table;
                string fileName = table.Name + DateTime.Now.ToString("_yyyyMMddHHmmss") + ".csv";
                var writerResult = await GetWriteFileStream(flatFile, EFlatFilePath.outgoing, fileName);

                if(!writerResult.Success)
                {
                    return writerResult;
                }

                //open a new filestream and write a headerrow
                _fileStream = writerResult.Value;
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

                return new ReturnValue(true, "", null);
            }
            catch(Exception ex)
            {
                return new ReturnValue(true, "The file could not be opened due to the following error: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> DataWriterFinish(Table table)
        {
            return await Task.Run(() =>
            {
                _fileWriter.Dispose();
                _fileStream.Dispose();
                _csvWriter.Dispose();

                return new ReturnValue(true);
            });
        }

        public override async Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancelToken)
        {
            try
            {
                Stopwatch timer = Stopwatch.StartNew();

                while(await reader.ReadAsync(cancelToken))
                {
                    if (cancelToken.IsCancellationRequested)
                        return new ReturnValue<long>(false, "Insert rows cancelled.", null, timer.ElapsedTicks);

                    string[] s = new string[reader.FieldCount];
                    for (int j = 0; j < reader.FieldCount; j++)
                    {
                        _csvWriter.WriteField(reader[j]);
                    }
                    _csvWriter.NextRecord();
                }
                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }
            catch (Exception ex)
            {
                return new ReturnValue<long>(false, "The file could not be written to due to the following error: " + ex.Message, ex);
            }
        }


        public override async Task<ReturnValue<List<string>>> GetDatabaseList(CancellationToken cancelToken)
        {
            return await GetFileShares(Server, Username, Password);
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table originalTable, CancellationToken cancelToken)
        {
            try
            {
                return await Task.Run(() =>
                {

                    var flatFile = (FlatFile)originalTable;

                    if (flatFile.FileFormat == null || flatFile.FileSample == null)
                    {
                        return new ReturnValue<Table>(false, "The properties have not been set to import the flat files structure.  Required properties are (FileFormat)FileFormat and (Stream)FileSample.", null);
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
                            return new ReturnValue<Table>(false, "The following error occurred opening the filestream: " + ex.Message, ex, null);
                        }
                    } else
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

                    return new ReturnValue<Table>(true, newFlatFile);
                });
            }
            catch(Exception ex)
            {
                return new ReturnValue<Table>(false, "The following error occurred when importing the file structure: " + ex.Message, ex);
            }
        }

        public override Task<ReturnValue<List<Table>>> GetTableList(CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            var flatFile = (FlatFile)table;
            var fileEnumeratorResult = await GetFileEnumerator(flatFile, FlatFile.EFlatFilePath.incoming, flatFile.FileMatchPattern);
            if(!fileEnumeratorResult.Success)
            {
                return fileEnumeratorResult;
            }

            var fileEnumerator = fileEnumeratorResult.Value;

            while(fileEnumerator.MoveNext())
            {
                var deleteResult = await DeleteFile(flatFile, FlatFile.EFlatFilePath.incoming, fileEnumerator.Current.FileName);
                if(!deleteResult.Success)
                {
                    return deleteResult;
                }
            }

            return new ReturnValue(true);
        }

        public override async Task<ReturnValue<Table>> InitializeTable(Table table, int position)
        {
			var flatFile = new FlatFile();
            table.CopyProperties(flatFile, false);
            
			//use the default paths.
			flatFile.UseCustomFilePaths = false;
            flatFile.AutoManageFiles = true;
            
            await CreateFilePaths(flatFile);
            
            return new ReturnValue<Table>(true, flatFile);
        }

        public override Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancelToken)
        {
            try
            {
                int rows = 0;
                Stopwatch timer = Stopwatch.StartNew();

                string fileName = table.Name + DateTime.Now.ToString("_yyyyMMddHHmmss") + ".csv";
                var flatFile = (FlatFile)table;

                var writerResult = await GetWriteFileStream(flatFile, EFlatFilePath.outgoing, fileName);

                //open a new filestream 
                using (var writer = writerResult.Value)
                using(var streamWriter = new StreamWriter(writer))
                using (var csv = new CsvWriter(streamWriter, flatFile.FileFormat))
                {

                    if (!(queries?.Count >= 0))
                        return new ReturnValue<Tuple<long, long>>(true, Tuple.Create(timer.Elapsed.Ticks, (long)0));

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
                        for (Int32 j = 0; j < query.InsertColumns.Count; j++)
                        {
                            csv.WriteField(query.InsertColumns[j].Value);
                        }
                        csv.NextRecord();
                        rows++;
                    }
                }

                LastWrittenFile = Filename;
                timer.Stop();
                return new ReturnValue<Tuple<long, long>>(true, Tuple.Create(timer.Elapsed.Ticks, (long)0)); //sometimes reader returns -1, when we want this to be error condition.
            }
            catch(Exception ex)
            {
                return new ReturnValue<Tuple<long, long>>(false, "The following error was encountered running the ExecuteInsert: " + ex.Message, ex);
            }
        }


        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            var timer = Stopwatch.StartNew();

            FlatFile flatFile = (FlatFile)table;
            using (var reader = new ReaderFlatFile(this, flatFile, true))
            {
                var openResult = await reader.Open(0, query, cancelToken);
                if (!openResult.Success)
                {
                    return new ReturnValue<object>(openResult);
                }

                var row = await reader.ReadAsync(cancelToken);
                if (row)
                {
                    var value = reader[query.Columns[0].Column.Name];
                    return new ReturnValue<object>(true, value);
                }
                else
                {
                    return new ReturnValue<object>(false, "No value was found.", null);
                }
            }
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
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
