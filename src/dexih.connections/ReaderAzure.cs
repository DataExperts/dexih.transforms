using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Net.Http;

namespace dexih.connections
{
    public class ReaderAzure : Transform
    {
        private bool _isOpen = false;

        private TableContinuationToken _token;

        private TableQuerySegment<DynamicTableEntity> _tableResult;
        private TableQuery<DynamicTableEntity> _tableQuery;

        CloudTable _tableReference;
        private int _currentReadRow;
        string[] _outputFields;

        private ConnectionAzure _connection;
        private Table _table;

        public ReaderAzure(Connection connection, Table table)
        {
            _connection = (ConnectionAzure)connection;
            _table = table;
        }

        public override async Task<ReturnValue> Open(SelectQuery query)
        {
            if (_isOpen)
            {
                return new ReturnValue(false, "The current connection is already open.", null);
            }

            CloudTableClient tableClient = _connection.GetCloudTableClient();
            _tableReference = tableClient.GetTableReference(_table.TableName);

            _tableQuery = new TableQuery<DynamicTableEntity>().Take(10);
            _tableQuery.SelectColumns = query.Columns.Select(c => c.Column).ToArray();
            _tableQuery.FilterString = _connection.BuildFilterString(query.Filters);
            _tableQuery.TakeCount = 1000;

            try
            {
                _tableResult = await _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token);
            }
            catch (StorageException ex)
            {
                string message = "Error reading Azure Storage table: " + _table.TableName + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation.ExtendedErrorInformation.ErrorMessage + ".";
                return new ReturnValue(false, message, ex);
            }

            _token = _tableResult.ContinuationToken;

            if (_tableResult != null && _tableResult.Any())
            {
                _outputFields = _table.Columns.Select(c => c.ColumnName).ToArray();
                return new ReturnValue(true);
            }
            else
                return new ReturnValue(false);
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
            try
            {
                if (_currentReadRow >= _tableResult.Count())
                {
                    if (_token == null)
                        return new ReturnValue<object[]>(false, null);
                    var test = _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token).Result;

                    _tableResult = _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token).Result;
                    _token = _tableResult.ContinuationToken;
                    _currentReadRow = 0;
                }

                DynamicTableEntity currentEntity = _tableResult.ElementAt(_currentReadRow);

                object[] row = GetRow(currentEntity);

                _currentReadRow++;

                return new ReturnValue<object[]>(true, row);
            }
            catch (Exception ex)
            {
                throw new Exception("The azure storage table reader failed due to the following error: " + ex.Message, ex);
            }
        }

        private object[] GetRow(DynamicTableEntity currentEntity)
        {
            object[] row = new object[_outputFields.Length];

            row[Array.IndexOf(_outputFields, "partitionKey")] = currentEntity.PartitionKey;
            row[Array.IndexOf(_outputFields, "rowKey")] = currentEntity.RowKey;
            row[Array.IndexOf(_outputFields, "Timestamp")] = currentEntity.Timestamp.ToString();

            foreach (var value in _tableResult.ElementAt(_currentReadRow).Properties)
            {
                if (value.Key != "rowKey" && value.Key != "partitionKey" && value.Key != "Timestamp")
                {
                    object returnValue = value.Value.PropertyAsObject;
                    if (returnValue == null)
                        row[Array.IndexOf(_outputFields, value.Key)] = DBNull.Value;
                    else
                        row[Array.IndexOf(_outputFields, value.Key)] = returnValue;
                }
            }

            return row;
        }

        public override bool CanLookupRowDirect { get; } = true;

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public override async Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters)
        {
            try
            {
                CloudTableClient tableClient = _connection.GetCloudTableClient();
                CloudTable cTable = tableClient.GetTableReference(_table.TableName);

                //Read the key fields from the table
                TableQuery tableQuery = new TableQuery();
                tableQuery.SelectColumns = _table.Columns.Select(c=>c.ColumnName).ToArray();
                tableQuery.FilterString = _connection.BuildFilterString(filters);
                tableQuery.Take(1);

                TableContinuationToken continuationToken = null;
                var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken);
                continuationToken = result.ContinuationToken;

                DynamicTableEntity currentEntity = result.ElementAt(_currentReadRow);
                object[] row = GetRow(currentEntity);

                return new ReturnValue<object[]>(true, row);
            }
            catch (Exception ex)
            {
                return new ReturnValue<object[]>(false, "The following error occurred when calling the Azure: " + ex.Message, ex);
            }
        }
    }
}
