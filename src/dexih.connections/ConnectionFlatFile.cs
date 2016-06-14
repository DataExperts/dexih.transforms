using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using dexih.connections.IO.Csv;
using System.IO;
using dexih.functions;
using System.Data.Common;
using static dexih.functions.DataType;
using dexih.transforms;

namespace dexih.connections
{
    public abstract class ConnectionFlatFile : Connection
    {


        public abstract Task<ReturnValue<List<string>>> GetFileShares(string serverName, string userName, string password);
        public abstract Task<ReturnValue> CreateDirectory(string rootDirectory, string subDirectory);
        public abstract Task<ReturnValue> MoveFile(string rootDirectory, string fromDirectory, string toDirectory, string fileName);
        public abstract Task<ReturnValue> DeleteFile(string rootDirectory, string subDirectory, string fileName);
        public abstract Task<ReturnValue<DexihFiles>> GetFileEnumerator(string mainDirectory, string subDirectory);
        public abstract Task<ReturnValue<List<DexihFileProperties>>> GetFileList(string mainDirectory, string subDirectory);
        public abstract Task<ReturnValue<Stream>> GetReadFileStream(Table table, string subDirectory, string fileName);
        public abstract Task<ReturnValue<Stream>> GetWriteFileStream(Table table, string subDirectory, string fileName);
        public abstract Task<ReturnValue> SaveFileStream(Table table, string fileName, Stream fileStream);
        public abstract Task<ReturnValue> TestFileConnection();


        public override string ServerHelp => "Path for the files (use \\\\server\\path format)";
//help text for what the server means for this description
        public override string DefaultDatabaseHelp => "";
//help text for what the default database means for this description
        public override bool AllowNtAuth => false;
        public override bool AllowUserPass => true;
        public override bool AllowDataPoint => false;
        public override bool AllowManaged => false;
        public override bool AllowPublish => true;
        public override bool CanBulkLoad => true;
        public override string DatabaseTypeName => "Flat Files";
        public override ECategory DatabaseCategory => ECategory.File;

        DexihFiles _files;
        Stream _fileStream;
        StreamWriter _fileWriter;

        CsvReader _csvReader;


        public override async Task<ReturnValue> CreateManagedTable(Table table, bool dropTable = false)
        {
            //create the subdirectories
            return await CreateDirectory((string)table.ExtendedProperties["FileRootPath"], (string)table.ExtendedProperties["FileIncomingPath"]);
        }

        public override async Task<ReturnValue> CreateDatabase(string DatabaseName)
        {
            ReturnValue returnValue;
            //create the subdirectories
            returnValue = await CreateDirectory((string)ServerName, "");
            return returnValue;
        }

        public async Task<ReturnValue> CreateFilePaths(Table table)
        {
            ReturnValue returnValue;
            //create the subdirectories
            returnValue = await CreateDirectory((string)table.ExtendedProperties["FileRootPath"], (string)table.ExtendedProperties["FileIncomingPath"]);
            if (returnValue.Success == false) return returnValue;
            returnValue = await CreateDirectory((string)table.ExtendedProperties["FileRootPath"], (string)table.ExtendedProperties["FileProcessedPath"]);
            if (returnValue.Success == false) return returnValue;
            returnValue = await CreateDirectory((string)table.ExtendedProperties["FileRootPath"], (string)table.ExtendedProperties["FileRejectedPath"]);
            return returnValue;
        }

        /// <summary>
        /// Adds a guid to the file name and moves it to the Incoming directory.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="fileName"></param>
        /// <param name="fromDirectory"></param>
        /// <param name="toDirectory"></param>
        /// <returns></returns>
        public async Task<ReturnValue> MoveFile(Table table, string fileName, string fromDirectory, string toDirectory)
        {
            return await MoveFile((string)table.ExtendedProperties["FileRootPath"], fromDirectory, toDirectory, fileName);
        }

        public async Task<ReturnValue> SaveIncomingFile(Table table, string fileName, Stream fileStream)
        {
            return await SaveFileStream(table, fileName, fileStream);
        }

        public async Task<ReturnValue<List<DexihFileProperties>>> GetIncomingFiles(Table table)
        {
            return await GetFileList((string)table.ExtendedProperties["FileRootPath"], (string)table.ExtendedProperties["FileIncomingPath"]);
        }

        public async Task<ReturnValue<List<DexihFileProperties>>> GetRejectedFiles(Table table)
        {
            return await GetFileList((string)table.ExtendedProperties["FileRootPath"], (string)table.ExtendedProperties["FileRejectedPath"]);
        }

        public async Task<ReturnValue<List<DexihFileProperties>>> GetProcessedFiles(Table table)
        {
            return await GetFileList((string)table.ExtendedProperties["FileRootPath"], (string)table.ExtendedProperties["FileProcessedPath"]);
        }

        public async Task<ReturnValue<List<DexihFileProperties>>> GetFileList(Table table, string subDirectory)
        {
            return await GetFileList((string)table.ExtendedProperties["FileRootPath"], subDirectory);
        }

        public async Task<ReturnValue> DeleteFile(Table table, string subDirectory, string fileName)
        {
            return await DeleteFile((string)table.ExtendedProperties["FileRootPath"], subDirectory, fileName);
        }

        public async Task<ReturnValue<Stream>> DownloadFile(Table table, string subDirectory, string fileName)
        {
            return await GetReadFileStream(table, subDirectory, fileName);
        }

        public override async Task<ReturnValue> DataWriterStart(Table table)
        {
            try
            {
                //open a new filestream and write a headerrow
                _fileStream = new MemoryStream();
                _fileWriter = new StreamWriter(_fileStream);

                string[] s = new string[table.Columns.Count];
                for (Int32 j = 0; j < table.Columns.Count; j++)
                {
                    s[j] = table.Columns[j].ColumnName;
                    if (s[j].Contains("\"")) //replace " with ""
                        s[j] = s[j].Replace("\"", "\"\"");
                    if (s[j].Contains("\"") || s[j].Contains(" ")) //add "'s around any string with space or "
                        s[j] = "\"" + s[j] + "\"";
                }
                await _fileWriter.WriteLineAsync(string.Join(",", s));

                return new ReturnValue(true, "", null);
            }
            catch(Exception ex)
            {
                return new ReturnValue(true, "The file could not be opened due to the following error: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> DataWriterFinish(Table table)
        {
            string archiveFileName = table.TableName + DateTime.Now.ToString("_yyyyMMddHHmmss") + ".csv";
            _fileStream.Position = 0;
            ReturnValue returnValue = await SaveFileStream(table, archiveFileName, _fileStream);

            _fileStream.Dispose();

            return returnValue;
        }

        protected override async Task<ReturnValue> WriteDataBulkInner(DbDataReader reader, Table table)
        {
            try
            {
                while(reader.Read())
//                for (int i = 0; i < sourceData.Rows.Count; i++)
                {
                    string[] s = new string[reader.FieldCount];
                    for (int j = 0; j < reader.FieldCount; j++)
                    {
                        s[j] = reader.GetString(j);
                        if (s[j].Contains("\"")) //replace " with ""
                            s[j] = s[j].Replace("\"", "\"\"");
                        if(s[j].Contains("\"") || s[j].Contains(" ")) //add "'s around any string with space or "
                            s[j] = "\"" + s[j] + "\"";
                    }
                    await _fileWriter.WriteLineAsync(string.Join(",", s));
                }
                return new ReturnValue(true, "", null);
            }
            catch (Exception ex)
            {
                return new ReturnValue(true, "The file could not be written to due to the following error: " + ex.Message, ex);
            }
        }

        // Override the read function to allow reader to loop through files.
        protected override ReturnValue<object[]> ReadRecord()
        {
            bool notfinished;
            try
            {
                notfinished = _csvReader.Read();
            }
            catch(Exception ex)
            {
                throw new Exception("The flatfile reader failed with the following message: " + ex.Message, ex);
            }

            if (notfinished == false)
            {
                var moveFileResult = MoveFile(CachedTable, _files.Current.FileName, (string)CachedTable.ExtendedProperties["FileIncomingPath"], (string)CachedTable.ExtendedProperties["FileProcessedPath"]).Result; //backup the completed file
                if(moveFileResult.Success == false)
                {
                    throw new Exception("The flatfile reader failed with the following message: " + moveFileResult.Message);
                }

                if (_files.MoveNext() == false)
                    OpenReader = false;
                else
                {
                    var fileStream = GetReadFileStream(CachedTable, (string)CachedTable.ExtendedProperties["FileIncomingPath"], _files.Current.FileName).Result;
                    if (fileStream.Success == false)
                        throw new Exception("The flatfile reader failed with the following message: " + fileStream.Message);

                    _csvReader = new CsvReader(new StreamReader(fileStream.Value), ((FileFormat)CachedTable.ExtendedProperties["FileFormat"]).Headers);
                    try
                    {
                        notfinished = _csvReader.Read();
                    }
                    catch (Exception ex)
                    {
                        throw new Exception("The flatfile reader failed with the following message: " + ex.Message, ex);
                    }
                    if (notfinished == false)
                        return ReadRecord(); // this creates a recurive loop to cater for empty files.
                }
            }

            if (notfinished)
            {
                object[] row = new object[CachedTable.Columns.Count];
                _csvReader.GetValues(row);
                return new ReturnValue<object[]>(true, row);
            }
            else
                return new ReturnValue<object[]>(false, null);

        }



        #region IRecord Interface //Overrides to add FileName field
        public override int FieldCount => _csvReader.FieldCount + 1;

        public override bool CanRunQueries => false;

        public override string GetName(int i)
        {
            if (i == _csvReader.FieldCount)
                return "FileName";
            return _csvReader.GetName(i);
        }
        public override int GetOrdinal(string name)
        {
            if (name == "FileName")
                return _csvReader.FieldCount;
            return _csvReader.GetOrdinal(name);
        }

        public override object GetValue(int i)
        {
            if (i == _csvReader.FieldCount)
                return _files.Current.FileName;
            return _csvReader[i];
        }
        #endregion


        protected override async Task<ReturnValue> DataReaderStartQueryInner(Table table, SelectQuery query)
        {
            if (OpenReader)
            {
                return new ReturnValue(false, "The file reader connection is already open.", null);
            }

            CachedTable = table;

            var fileEnumerator = await GetFileEnumerator((string)table.ExtendedProperties["FileRootPath"], (string)table.ExtendedProperties["FileIncomingPath"]);
            if (fileEnumerator.Success == false)
                return fileEnumerator;

            _files = fileEnumerator.Value;

            if (_files.MoveNext() == false)
            {
                return new ReturnValue(false, "There were no files in the incomming directory.", null);
            }

            var fileStream = await GetReadFileStream(table, (string)table.ExtendedProperties["FileIncomingPath"], _files.Current.FileName);
            if (fileStream.Success == false)
                return fileStream;

            _csvReader = new CsvReader(new StreamReader(fileStream.Value), ((FileFormat)table.ExtendedProperties["FileFormat"]).Headers);

            return new ReturnValue(true);
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            return await GetFileShares(ServerName, UserName, Password);
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, object> Properties)
        {
            try
            {
                if (Properties == null || Properties["FileFormat"] == null || !(Properties["FileFormat"] is FileFormat) || Properties["FileStream"] == null || !(Properties["FileStream"] is Stream) )
                {
                    return new ReturnValue<Table>(false, "The properties have not been set to import the flat files structure.  Required properties are (FileFormat)FileFormat and (Stream)FileStream.", null);
                }

                FileFormat fileFormat = (FileFormat) Properties["FileFormat"];
                Stream fileStream = (Stream) Properties["FileStream"];

                string[] headers;
                try
                {
                    CsvReader csv = await Task.Run(() => new CsvReader(new StreamReader(fileStream), fileFormat.Headers));
                    headers = await Task.Run(() => csv.GetFieldHeaders());
                    fileStream.Dispose();
                }
                catch(Exception ex)
                {
                    return new ReturnValue<Table>(false, "The following error occurred opening the filestream: " + ex.Message, ex, null);
                }

                //The new datatable that will contain the table schema
                Table table = new Table(tableName);
                table.Columns.Clear();
                table.LogicalName = table.TableName;
                table.Description = "";
                table.ExtendedProperties["FileFormat"] = fileFormat;

                TableColumn col;

                foreach (string field in headers)
                {
                    col = new TableColumn();

                    //add the basic properties
                    col.ColumnName = field;
                    col.LogicalName = field;
                    col.IsInput = false;
                    col.DataType = ETypeCode.String;
                    col.DeltaType = TableColumn.EDeltaType.TrackingField;
                    col.Description = "";
                    col.AllowDbNull = true;
                    col.IsUnique = false;

                    table.Columns.Add(col);
                }

                col = new TableColumn();

                //add the basic properties
                col.ColumnName = "FileName";
                col.LogicalName = "FileName";
                col.IsInput = false;
                col.DataType = ETypeCode.String;
                col.DeltaType = TableColumn.EDeltaType.FileName;
                col.Description = "The name of the file the record was loaded from.";
                col.AllowDbNull = false;
                col.IsUnique = false;

                table.Columns.Add(col);

                return new ReturnValue<Table>(true, table);
            }
            catch(Exception ex)
            {
                return new ReturnValue<Table>(false, "The following error occurred when importing the file structure: " + ex.Message, ex);
            }
        }

        public override Task<ReturnValue<List<string>>> GetTableList()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue> TruncateTable(Table table)
        {
            throw new NotImplementedException();
        }

        public override string GetCurrentFile()
        {
            return _files.Current.FileName;
        }

        public override ReturnValue ResetTransform()
        {
            throw new NotImplementedException();
        }

        public override bool Initialize()
        {
            throw new NotImplementedException();
        }

        public override string Details()
        {
            return "Flat File: " + CachedTable.TableName;

        }



        public override async Task<ReturnValue> AddMandatoryColumns(Table table, int position)
        {
            await Task.Run(() =>
            {
                //create path for the file management.
                string rootPath = table.TableName + Guid.NewGuid().ToString();
                table.ExtendedProperties["FileIncomingPath"] = "Archives";
                table.ExtendedProperties["FileRootPath"] = rootPath;
            });
            return new ReturnValue(true);
        }

        public override Task<ReturnValue<int>> ExecuteUpdateQuery(Table table, List<UpdateQuery> query)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteDeleteQuery(Table table, List<DeleteQuery> query)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteInsertQuery(Table table, List<InsertQuery> query)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query)
        {
            throw new NotImplementedException();
        }


    }
}
