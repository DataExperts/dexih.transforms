using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Threading;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.connections.azure
{
    public class ReaderAzure : Transform
    {
        private bool _isOpen = false;

        private TableContinuationToken _token;

        private TableQuerySegment<DynamicTableEntity> _tableResult;
        private TableQuery<DynamicTableEntity> _tableQuery;

        private CloudTable _tableReference;
        private int _currentReadRow;

        private readonly ConnectionAzureTable _connection;

        public ReaderAzure(Connection connection, Table table)
        {
            _connection = (ConnectionAzureTable)connection;
            CacheTable = table;
        }

        protected override void Dispose(bool disposing)
        {
            _isOpen = false;

            base.Dispose(disposing);
        }

        public override async Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;
            if (_isOpen)
            {
                throw new ConnectionException($"The current connection is already open");
            }

            CloudTableClient tableClient = _connection.GetCloudTableClient();
            _tableReference = tableClient.GetTableReference(CacheTable.Name);

            _tableQuery = new TableQuery<DynamicTableEntity>().Take(1000);

            if (query?.Columns?.Count > 0)
                _tableQuery.SelectColumns = query.Columns.Select(c => c.Column.Name).ToArray();
            else
                _tableQuery.SelectColumns = CacheTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.IgnoreField).Select(c => c.Name).ToArray();

            if (query?.Filters != null)
                _tableQuery.FilterString = _connection.BuildFilterString(query.Filters);

            if(query?.Rows > 0 && query?.Rows < 1000)
                _tableQuery.TakeCount = query.Rows;

            try
            {
                _tableResult = await _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token);
            }
            catch (StorageException ex)
            {
                string message = "Error reading Azure Storage table: " + CacheTable.Name + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation?.ExtendedErrorInformation?.ErrorMessage + ".";
                throw new ConnectionException(message, ex);
            }

            _token = _tableResult.ContinuationToken;

            return true;
        }

        public override string Details()
        {
            return "AzureConnection";
        }

        public override bool InitializeOutputFields()
        {
            return true;
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
            try
            {
                if (_tableResult.Count() == 0)
                    return null;

                if (_currentReadRow >= _tableResult.Count())
                {
                    if (_token == null)
                        return null;

                    _tableResult = await _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token);
                    if (_tableResult.Count() == 0)
                        return null;

                    _token = _tableResult.ContinuationToken;
                    _currentReadRow = 0;
                }

                DynamicTableEntity currentEntity = _tableResult.ElementAt(_currentReadRow);

                object[] row = GetRow(currentEntity);

                _currentReadRow++;

                return row;
            }
            catch (Exception ex)
            {
                throw new ConnectionException("The azure storage table reader failed due to the following error: " + ex.Message, ex);
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
                {
                    row[CacheTable.GetOrdinal(value.Key)] = _connection.ConvertEntityProperty(CacheTable[value.Key].Datatype, returnValue);
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
        public override async Task<object[]> LookupRowDirect(List<Filter> filters, CancellationToken cancelToken)
        {
            try
            {
                CloudTableClient tableClient = _connection.GetCloudTableClient();
                CloudTable cTable = tableClient.GetTableReference(CacheTable.Name);

                //Read the key fields from the table
                TableQuery tableQuery = new TableQuery();
                tableQuery.SelectColumns = CacheTable.Columns.Select(c=>c.Name).ToArray();
                tableQuery.FilterString = _connection.BuildFilterString(filters);
                tableQuery.Take(1);

                TableContinuationToken continuationToken = null;
                var result = await cTable.ExecuteQuerySegmentedAsync(tableQuery, continuationToken);
                continuationToken = result.ContinuationToken;

                DynamicTableEntity currentEntity = result.ElementAt(_currentReadRow);
                object[] row = GetRow(currentEntity);

                return row;
            }
            catch (Exception ex)
            {
                throw new ConnectionException("The azure table lookup failed due to the following error: " + ex.Message, ex);
            }
        }
    }
}
