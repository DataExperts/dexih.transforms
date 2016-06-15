using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using dexih.connections.IO.Csv;
using System.IO;

namespace dexih.connections
{
    public class ReaderFlatFile : Transform
    {
        private bool _isOpen = false;

        DexihFiles _files;
        CsvReader _csvReader;

        public ReaderFlatFile(Connection connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;
        }

        public override async Task<ReturnValue> Open(SelectQuery query)
        {
            if (_isOpen)
            {
                return new ReturnValue(false, "The file reader connection is already open.", null);
            }

            var fileEnumerator = await ((ConnectionFlatFile)ReferenceConnection).GetFileEnumerator((string)CacheTable.ExtendedProperties["FileRootPath"], (string)CacheTable.ExtendedProperties["FileIncomingPath"]);
            if (fileEnumerator.Success == false)
                return fileEnumerator;

            _files = fileEnumerator.Value;

            if (_files.MoveNext() == false)
            {
                return new ReturnValue(false, "There were no files in the incomming directory.", null);
            }

            var fileStream = await ((ConnectionFlatFile)ReferenceConnection).GetReadFileStream(CacheTable, (string)CacheTable.ExtendedProperties["FileIncomingPath"], _files.Current.FileName);
            if (fileStream.Success == false)
                return fileStream;

            _csvReader = new CsvReader(new StreamReader(fileStream.Value), ((FileFormat)CacheTable.ExtendedProperties["FileFormat"]).Headers);

            return new ReturnValue(true);
        }

        public override string Details()
        {
            return "SqlConnection";
        }

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override ReturnValue ResetTransform()
        {
            throw new NotImplementedException();
        }

        protected override ReturnValue<object[]> ReadRecord()
        {
            bool notfinished;
            try
            {
                notfinished = _csvReader.Read();
            }
            catch (Exception ex)
            {
                throw new Exception("The flatfile reader failed with the following message: " + ex.Message, ex);
            }

            if (notfinished == false)
            {
                var moveFileResult = ((ConnectionFlatFile)ReferenceConnection).MoveFile(CacheTable, _files.Current.FileName, (string)CacheTable.ExtendedProperties["FileIncomingPath"], (string)CacheTable.ExtendedProperties["FileProcessedPath"]).Result; //backup the completed file
                if (moveFileResult.Success == false)
                {
                    throw new Exception("The flatfile reader failed with the following message: " + moveFileResult.Message);
                }

                if (_files.MoveNext() == false)
                    _isOpen = false;
                else
                {
                    var fileStream = ((ConnectionFlatFile)ReferenceConnection).GetReadFileStream(CacheTable, (string)CacheTable.ExtendedProperties["FileIncomingPath"], _files.Current.FileName).Result;
                    if (fileStream.Success == false)
                        throw new Exception("The flatfile reader failed with the following message: " + fileStream.Message);

                    _csvReader = new CsvReader(new StreamReader(fileStream.Value), ((FileFormat)CacheTable.ExtendedProperties["FileFormat"]).Headers);
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
                object[] row = new object[CacheTable.Columns.Count];
                _csvReader.GetValues(row);
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
        public override Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters)
        {
            throw new NotSupportedException("Lookup not supported with flat files.");
        }
    }
}
