using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.connections
{
    public class ReaderSQL : Transform
    {
        bool isOpen = false;

        public override ReturnValue Open(List<Filter> filters = null, List<Sort> sorts = null)
        {
            if (isOpen)
            {
                return new ReturnValue(false, "The reader is already open.", null);
            }

            CachedTable = table;

            ReturnValue<SqlConnection> connection = await NewConnection();
            if (connection.Success == false)
            {
                return connection;
            }

            _connection = connection.Value;

            string sql = BuildSelectQuery(table, query);
            SqlCommand cmd = new SqlCommand(sql, _connection);

            try
            {
                _sqlReader = await cmd.ExecuteReaderAsync();
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The connection reader for the sqlserver table " + table.TableName + " could failed due to the following error: " + ex.Message, ex);
            }

            if (_sqlReader == null)
            {
                return new ReturnValue(false, "The connection reader for the sqlserver table " + table.TableName + " return null for an unknown reason.  The sql command was " + sql, null);
            }
            else
            {
                isOpen = true;
                return new ReturnValue(true, "", null);
            }
        }

        public override string Details()
        {
            throw new NotImplementedException();
        }

        public override bool InitializeOutputFields()
        {
            throw new NotImplementedException();
        }

        public override ReturnValue ResetTransform()
        {
            throw new NotImplementedException();
        }

        protected override ReturnValue<object[]> ReadRecord()
        {
            throw new NotImplementedException();
        }
    }
}
