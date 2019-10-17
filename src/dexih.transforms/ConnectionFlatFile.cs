﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using dexih.functions;
using System.Data.Common;
using System.Threading;
using System.IO.Compression;
using System.Linq;
using CsvHelper;
using System.Text.RegularExpressions;
using Dexih.Utils.CopyProperties;
using dexih.transforms.Exceptions;
using static Dexih.Utils.DataType.DataType;
using dexih.functions.Query;
using dexih.transforms.File;
using Dexih.Utils.DataType;

namespace dexih.transforms
{
    public abstract class ConnectionFlatFile : Connection
    {
        public abstract Task<List<string>> GetFileShares();
        public abstract Task<bool> CreateDirectory(FlatFile file, EFlatFilePath path);
        public abstract Task<bool> MoveFile(FlatFile file, EFlatFilePath fromPath, EFlatFilePath toPath, string fileName);
        public abstract Task<bool> DeleteFile(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<DexihFiles> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern);
        public abstract Task<List<DexihFileProperties>> GetFileList(FlatFile file, EFlatFilePath path);
        public abstract Task<Stream> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<Stream> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName);
        public abstract Task<bool> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream fileStream);
        public abstract Task<bool> TestFileConnection();
        public abstract string GetFullPath(FlatFile file, EFlatFilePath path);
        
        public override bool CanBulkLoad => true;
        public override bool CanSort => false;
        public override bool CanFilter => true;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanAggregate => false;
        public override bool CanUseBinary => false;
        public override bool CanUseArray => false;
        public override bool CanUseJson => false;
        public override bool CanUseXml => false;
        public override bool CanUseCharArray => false;
        public override bool CanUseSql => false;
        public override bool CanUseDbAutoIncrement => false;
        public override bool DynamicTableCreation => true;

        private Stream _fileStream;
        private StreamWriter _fileWriter;
        private CsvWriter _csvWriter;

        public string LastWrittenFile { get; protected set; } = "";
        
		public override async Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
        {
			var flatFile = (FlatFile)table;
            //create the subdirectories
            await CreateDirectory(flatFile, EFlatFilePath.Incoming);
        }

        public override async Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            DefaultDatabase = databaseName;
            //create the subdirectories
            await CreateDirectory(null, EFlatFilePath.None);
        }

        public async Task<bool> CreateFilePaths(FlatFile flatFile)
        {
            //create the subdirectories
            var returnValue = await CreateDirectory(flatFile, EFlatFilePath.Incoming);
            if (returnValue == false) return false;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.Outgoing);
            if (returnValue == false) return false;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.Processed);
            if (returnValue == false) return false;
            returnValue = await CreateDirectory(flatFile, EFlatFilePath.Rejected);
            return returnValue;
        }
        
        /// <summary>
        /// Saves the stream to the path.
        /// If the file is a zip or gz, they will be decompressed and saved.
        /// </summary>
        /// <param name="file"></param>
        /// <param name="path"></param>
        /// <param name="fileName"></param>
        /// <param name="stream"></param>
        /// <returns></returns>
        /// <exception cref="ConnectionException"></exception>
        public async Task<bool> SaveFiles(FlatFile file, EFlatFilePath path, string fileName, Stream stream)
        {
            try
            {
                await CreateDirectory(file, path);

                var fileNameExtension = Path.GetExtension(fileName);

				if(fileNameExtension == ".zip")
                {
                    var result = true;
					using (var archive = new ZipArchive(stream, ZipArchiveMode.Read))
	                {
                        foreach(var entry in archive.Entries)
                        {
                            if(string.IsNullOrEmpty(entry.Name) || entry.FullName.StartsWith("__MACOSX") || entry.Length == 0) continue;
                            result = result & await SaveFileStream(file, path, entry.Name, entry.Open());
                        }

	                }
					return result;
				} else if (fileNameExtension == ".gz")
                {
                    using (var decompressionStream = new GZipStream(stream, CompressionMode.Decompress))
                    {
                        var newFileName = fileName.Substring(0, fileName.Length - 3);
                        return await SaveFileStream(file, path, newFileName, decompressionStream);
                    }
                }
				else 
				{
                    return await SaveFileStream(file, path, fileName, stream);
				}
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred saving file stream of file {fileName} at {path}.  {ex.Message}", ex);
            }
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
                            await fileStreamResult.CopyToAsync(fileEntryStream);
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

                if (flatFile.FormatType != ETypeCode.Text)
                {
                    //TODO Add support for flatfile writing to xml/json service.
                    throw new ConnectionException("The flatfile writer currently only supports FormatTypes of Text for writing.");
                }
                
                var fileName = table.Name + DateTime.Now.ToString("_yyyyMMddHHmmss") + ".csv";
                var writerResult = await GetWriteFileStream(flatFile, EFlatFilePath.Outgoing, fileName);

                //open a new filestream and write a headerrow
                _fileStream = writerResult ?? throw new ConnectionException($"Flat file write failed, could not get a write stream for {flatFile.Name}.");
                
                _fileWriter = new StreamWriter(_fileStream);
                _csvWriter = new CsvWriter(_fileWriter);


                if (flatFile.FileConfiguration.HasHeaderRecord)
                {
                    for (var j = 0; j < table.Columns.Count; j++)
                    {
                        _csvWriter.WriteField(table.Columns[j].Name);
                    }
                    await _csvWriter.NextRecordAsync();
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
        
        public override async Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken = default)
        {
            try
            {
                var oridinals = new int[table.Columns.Count];
                for(var i = 0; i < table.Columns.Count; i++)
                {
                    oridinals[i] = reader.GetOrdinal(table.Columns[i].Name);
                }
                
                while(await reader.ReadAsync(cancellationToken))
                {
                    if (cancellationToken.IsCancellationRequested)
                    {
                        throw new ConnectionException("Insert bulk operation cancelled.");
                    }

                    foreach(var ordinal in oridinals)
                    {
                        if (ordinal == -1)
                        {
                            _csvWriter.WriteField(null);
                        }
                        else
                        {
                            _csvWriter.WriteField(reader[ordinal]);
                        }
                    }
                    await _csvWriter.NextRecordAsync();
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Insert bulk operation failed.  {ex.Message}", ex);
            }
        }


        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            return await GetFileShares();
        }

        public override async Task<Table> GetSourceTableInfo(Table originalTable, CancellationToken cancellationToken = default)
        {
            try
            {
                var flatFile = (FlatFile)originalTable;

                if (flatFile.FileSample == null)
                {
                    throw new ConnectionException($"The properties have not been set to import the flat files structure.  Required properties are (FileFormat)FileFormat and (Stream)FileSample.");
                }

                var stream = new MemoryStream();
                var writer = new StreamWriter(stream);
                await writer.WriteAsync(flatFile.FileSample);
                await writer.FlushAsync();
                stream.Position = 0;

                FileHandlerBase fileHandler = null;

                switch (flatFile.FormatType)
                {
                    case ETypeCode.Json:
                        fileHandler = new FileHandlerJson(flatFile, flatFile.RowPath);
                        break;
                    case ETypeCode.Text:
                        fileHandler = new FileHandlerText(flatFile, flatFile.FileConfiguration);
                        break;
                    case ETypeCode.Xml:
                        fileHandler = new FileHandlerXml(flatFile, flatFile.RowPath);
                        break;
                   default:
                       throw new ConnectionException($"The source type {flatFile.FormatType} is not currently supported.");        
                }

                var columns = await fileHandler.GetSourceColumns(stream);

                //The new datatable that will contain the table schema
                var newFlatFile = flatFile.CopyFlatFile();
//                flatFile.Name = originalTable.Name;
//                newFlatFile.Columns.Clear();
//                newFlatFile.LogicalName = newFlatFile.Name;
//                newFlatFile.Description = "";

                foreach (var column in columns)
                {
                    newFlatFile.Columns.Add(column);
                }

                var col = new TableColumn()
                {

                    //add the basic properties
                    Name = "FileName",
                    LogicalName = "FileName",
                    IsInput = false,
                    DataType = ETypeCode.String,
                    DeltaType = EDeltaType.FileName,
                    Description = "The name of the file the record was loaded from.",
                    AllowDbNull = false,
                    IsUnique = false
                };
                newFlatFile.Columns.Add(col);

                return newFlatFile;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to import the file structure. {ex.Message}", ex);
            }
        }

        public override Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override async Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
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
        }

        public override async Task<Table> InitializeTable(Table table, int position)
        {
			var flatFile = new FlatFile();
            table.CopyProperties(flatFile, false);
            
			//use the default paths.
			flatFile.UseCustomFilePaths = false;
            flatFile.AutoManageFiles = true;
            flatFile.FormatType = ETypeCode.Text;
            
            await CreateFilePaths(flatFile);

            return flatFile;
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteDelete(Table table, List<DeleteQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override async Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
            try
            {
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
                        foreach (var column in table.Columns)
                        {
                            csv.WriteField(column.Name);
                        }
                        await csv.NextRecordAsync();
                    }

                    foreach (var query in queries)
                    {
                        foreach (var column in table.Columns)
                        {
                            var insertColumn = query.InsertColumns.SingleOrDefault(c => c.Column.Name == column.Name);
                            if (insertColumn == null)
                            {
                                csv.WriteField(null);
                            }
                            else
                            {
                                csv.WriteField(ConvertForWrite(insertColumn.Column, insertColumn.Value).value);    
                            }
                        }
                        
                        await csv.NextRecordAsync();
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


        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
        {
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

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
			var flatFile = (FlatFile)table;
            var reader = new ReaderFlatFile(this, flatFile, previewMode);
            return reader;
        }
        
        /// <summary>
        /// Tests if a file matches search pattern
        /// </summary>
        /// <param name="fileName"></param>
        /// <param name="fileMask"></param>
        /// <returns></returns>
        protected bool FitsMask(string fileName, string fileMask)
        {
            var pattern =
                '^' +
                Regex.Escape(fileMask.Replace(".", "__DOT__")
                        .Replace("*", "__STAR__")
                        .Replace("?", "__QM__"))
                    .Replace("__DOT__", "[.]")
                    .Replace("__STAR__", ".*")
                    .Replace("__QM__", ".")
                + '$';
            return new Regex(pattern, RegexOptions.IgnoreCase).IsMatch(fileName);
        }


    }
}
