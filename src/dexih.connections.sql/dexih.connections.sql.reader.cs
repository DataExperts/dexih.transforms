using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;
using System.Threading;

namespace dexih.connections.sql
{
    public class ReaderSQL : Transform
    {
        private bool _isOpen = false;
        private DbDataReader _sqlReader;
        private DbConnection _sqlConnection;

        public ReaderSQL(ConnectionSql connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;
        }

        protected override void Dispose(bool disposing)
        {
            if (_sqlReader != null)
                _sqlReader.Dispose();

            if (_sqlConnection != null)
                _sqlConnection.Dispose();

            base.Dispose(disposing);
        }

        public override async Task<ReturnValue> Open(SelectQuery query)
        {
            if (_isOpen)
            {
                return new ReturnValue(false, "The reader is already open.", null);
            }

            var connectionResult = await ((ConnectionSql)ReferenceConnection).NewConnection();
            if(!connectionResult.Success)
            {
                return new ReturnValue(false, "The connection reader for the table " + CacheTable.TableName + " could failed due to the following error: " + connectionResult.Message, connectionResult.Exception);
            }

            _sqlConnection = connectionResult.Value;

            var readerResult = await ReferenceConnection.GetDatabaseReader(CacheTable, _sqlConnection, query);

            if (!readerResult.Success)
            {
                return new ReturnValue(false, "The connection reader for the table " + CacheTable.TableName + " could failed due to the following error: " + readerResult.Message, readerResult.Exception);
            }

            _sqlReader = readerResult.Value;

            _isOpen = true;
            return new ReturnValue(true, "", null);
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
            if (! await _sqlReader.ReadAsync())
                return new ReturnValue<object[]>(false, null);

            //load the new row up, converting datatypes where neccessary.
            object[] row = new object[CacheTable.Columns.Count];
            for (int i = 0; i < _sqlReader.FieldCount; i++)
            {
                int ordinal = CacheTable.GetOrdinal(_sqlReader.GetName(i));
                var returnValue = DataType.TryParse(CacheTable.Columns[ordinal].DataType, _sqlReader[i]);
                if (!returnValue.Success)
                    return new ReturnValue<object[]>(returnValue);

                row[ordinal] = returnValue.Value;
            }
            return new ReturnValue<object[]>(true, row);
        }

        public override bool CanLookupRowDirect { get; } = true;

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public override async Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters)
        {
            SelectQuery query = new SelectQuery()
            {
                Columns = CacheTable.Columns.Where(c => c.DeltaType != TableColumn.EDeltaType.IgnoreField).Select(c => new SelectColumn(c.ColumnName)).ToList(),
                Filters = filters,
            };

            ReturnValue<DbConnection> connectionResult = await ((ConnectionSql)ReferenceConnection).NewConnection();
            if (!connectionResult.Success)
            {
                return new ReturnValue<object[]>(false, "The connection reader for the table " + CacheTable.TableName + " could failed due to the following error: " + connectionResult.Message, connectionResult.Exception);
            }

            using (var connection = connectionResult.Value)
            {
                var readerResult = await ReferenceConnection.GetDatabaseReader(CacheTable, connection, query);
                if (!readerResult.Success)
                {
                    return new ReturnValue<object[]>(false, "The connection reader for the table " + CacheTable.TableName + " could failed due to the following error: " + readerResult.Message, readerResult.Exception);
                }

                using (var reader = readerResult.Value)
                {
                    if (await reader.ReadAsync())
                    {
                        object[] values = new object[CacheTable.Columns.Count];
                        reader.GetValues(values);
                        return new ReturnValue<object[]>(true, values);
                    }
                    else
                        return new ReturnValue<object[]>(false, "The lookup query for " + CacheTable.TableName + " return no rows.", null);
                }
            }
        }
    }
}
