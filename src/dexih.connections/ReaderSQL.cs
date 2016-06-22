using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Data.Common;

namespace dexih.connections
{
    public class ReaderSQL : Transform
    {
        private bool _isOpen = false;
        private DbDataReader _sqlReader;

        public ReaderSQL(Connection connection, Table table)
        {
            ReferenceConnection = connection;
            CacheTable = table;
        }

        public override async Task<ReturnValue> Open(SelectQuery query)
        {
            if (_isOpen)
            {
                return new ReturnValue(false, "The reader is already open.", null);
            }

            var readerResult = await ReferenceConnection.GetDatabaseReader(CacheTable, query);

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

        protected override ReturnValue<object[]> ReadRecord()
        {
            if (!_sqlReader.Read())
                return new ReturnValue<object[]>(false, null);

            object[] row = new object[CacheTable.Columns.Count];
            _sqlReader.GetValues(row);
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

            var readerResult = await ReferenceConnection.GetDatabaseReader(CacheTable, query);

            if (!readerResult.Success)
            {
                return new ReturnValue<object[]>(false, "The connection reader for the table " + CacheTable.TableName + " could failed due to the following error: " + readerResult.Message, readerResult.Exception);
            }

            var reader = readerResult.Value;

            if (reader.Read())
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
