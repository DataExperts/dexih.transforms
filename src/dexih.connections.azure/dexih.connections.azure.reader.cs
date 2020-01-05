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
using Dexih.Utils.DataType;

namespace dexih.connections.azure
{
    public class ReaderAzure : Transform
    {
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
        
        public override string TransformName { get; } = "Azure Reader";
        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            if (IsOpen)
            {
                throw new ConnectionException($"The current connection is already open");
            }

            IsOpen = true;
            SelectQuery = requestQuery;
            GeneratedQuery = GetGeneratedQuery(requestQuery);

            var tableClient = _connection.GetCloudTableClient();
            _tableReference = tableClient.GetTableReference(CacheTable.Name);

            _tableQuery = new TableQuery<DynamicTableEntity>().Take(1000);

            if (GeneratedQuery?.Columns?.Count > 0)
                _tableQuery.SelectColumns = GeneratedQuery.Columns.Select(c => c.Column.Name).ToArray();
            else
                _tableQuery.SelectColumns = CacheTable.Columns.Where(c => c.DeltaType != EDeltaType.IgnoreField).Select(c => c.Name).ToArray();

            if (GeneratedQuery?.Filters != null)
                _tableQuery.FilterString = _connection.BuildFilterString(GeneratedQuery.Filters);

            if(GeneratedQuery?.Rows > 0 && GeneratedQuery?.Rows < 1000)
                _tableQuery.TakeCount = GeneratedQuery.Rows;

            try
            {
                _tableResult = await _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token);
            }
            catch (StorageException ex)
            {
                var message = "Error reading Azure Storage table: " + CacheTable.Name + ".  Error Message: " + ex.Message + ".  The extended message:" + ex.RequestInformation?.ExtendedErrorInformation?.ErrorMessage + ".";
                throw new ConnectionException(message, ex);
            }

            _token = _tableResult.ContinuationToken;

            return true;
        }


        public override bool ResetTransform()
        {
            if (IsOpen)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            try
            {
                if (!_tableResult.Any())
                    return null;

                if (_currentReadRow >= _tableResult.Count())
                {
                    if (_token == null)
                        return null;

                    _tableResult = await _tableReference.ExecuteQuerySegmentedAsync(_tableQuery, _token);
                    if (!_tableResult.Any())
                        return null;

                    _token = _tableResult.ContinuationToken;
                    _currentReadRow = 0;
                }

                var currentEntity = _tableResult.ElementAt(_currentReadRow);

                var row = GetRow(currentEntity);

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
            var row = new object[CacheTable.Columns.Count];

            var partitionKeyOrdinal = CacheTable.GetOrdinal(EDeltaType.PartitionKey);
            if(partitionKeyOrdinal >= 0)
                row[partitionKeyOrdinal] = currentEntity.PartitionKey;

            var rowKeyOrdinal = CacheTable.GetOrdinal(EDeltaType.RowKey);
            if (rowKeyOrdinal >= 0)
                row[rowKeyOrdinal] = currentEntity.RowKey;

            var timestampOrdinal = CacheTable.GetOrdinal(EDeltaType.TimeStamp);
            if (timestampOrdinal >= 0)
                row[timestampOrdinal] = currentEntity.Timestamp;

            foreach (var value in currentEntity.Properties)
            {
                var column = CacheTable[value.Key];
                var returnValue = value.Value.PropertyAsObject;
                row[CacheTable.GetOrdinal(value.Key)] = Operations.Parse(column.DataType, column.Rank, returnValue);

                //if (returnValue == null)
                //    row[CacheTable.GetOrdinal(value.Key)] = DBNull.Value;
                //else
                //{
                //    row[CacheTable.GetOrdinal(value.Key)] = _connection.ConvertEntityProperty(CacheTable[value.Key].DataType, returnValue);
                //}
            }

            return row;
        }

        public override async Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            Reset();
            Dispose();
            return await Open(auditKey, query, cancellationToken);
        }
    }
}
