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
using System.Threading;

namespace dexih.connections.azure
{
    public class ReaderAzure : Transform
    {
        private bool _isOpen = false;

        private TableContinuationToken _token;

        private TableQuerySegment<DynamicTableEntity> _tableResult;
        private TableQuery<DynamicTableEntity> _tableQuery;

        CloudTable _tableReference;
        private int _currentReadRow;

        private ConnectionAzureTable _connection;

        public ReaderAzure(Connection connection, Table table)
        {
            _connection = (ConnectionAzureTable)connection;
            CacheTable = table;
        }

        public override async Task<ReturnValue> Open(SelectQuery query)
        {
            if (_isOpen)
            {
                return new ReturnValue(false, "The current connection is already open.", null);
            }

            CloudTableClient tableClient = _connection.GetCloudTableClient();
            _tableReference = tableClient.GetTableReference(CacheTable.TableName);

            _tableQuery = new TableQuery<DynamicTableEntity>().Take(10);

            if (query?.Columns?.Count > 0)
                _tableQuery.SelectColumns = query.Columns.Select(c => c.Column).ToArray();
            else
                _tableQuery.SelectColumns = CacheTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.IgnoreField).Select(c => c.ColumnName).ToArray();

            if (query?.Filters != null)
                _tableQuery.FilterString = _connection.BuildFilterString(query.Filters);

            if(query?.Rows > 0)
                _tableQuery.TakeCount = query.Rows;

            try
            {
                _tableResult = await _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token);
            }
            catch (StorageException ex)
            {
                string message = "Error reading Azure Storage table: " + CacheTable.TableName + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation.ExtendedErrorInformation.ErrorMessage + ".";
                return new ReturnValue(false, message, ex);
            }

            _token = _tableResult.ContinuationToken;

            if (_tableResult != null && _tableResult.Any())
            {
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
            if (_isOpen)
            {
                return new ReturnValue(true);
            }
            else
                return new ReturnValue(false, "The sql reader can not be reset", null);

        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            try
            {
                if (_currentReadRow >= _tableResult.Count())
                {
                    if (_token == null)
                        return new ReturnValue<object[]>(false, null);

                    _tableResult = await _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token);
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
            object[] row = new object[CacheTable.Columns.Count];

            int partitionKeyOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzurePartitionKey);
            if(partitionKeyOrdinal >= 0)
                row[partitionKeyOrdinal] = currentEntity.PartitionKey;

            int rowKeyOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.AzureRowKey);
            if (rowKeyOrdinal >= 0)
                row[rowKeyOrdinal] = currentEntity.RowKey;

            int timestampOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.TimeStamp);
            if (timestampOrdinal >= 0)
                row[timestampOrdinal] = currentEntity.Timestamp;

            foreach (var value in currentEntity.Properties)
            {
                object returnValue = value.Value.PropertyAsObject;
                if (returnValue == null)
                    row[CacheTable.GetOrdinal(value.Key)] = DBNull.Value;
                else
                    row[CacheTable.GetOrdinal(value.Key)] = _connection.ConvertEntityProperty(CacheTable[value.Key].DataType, returnValue);
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
                CloudTable cTable = tableClient.GetTableReference(CacheTable.TableName);

                //Read the key fields from the table
                TableQuery tableQuery = new TableQuery();
                tableQuery.SelectColumns = CacheTable.Columns.Select(c=>c.ColumnName).ToArray();
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
