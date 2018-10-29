using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{

    /// <summary>
    /// This reader can be used to convert any data into datatypes supported by a target connection.
    /// </summary>
    public class ReaderConvertDataTypes: Transform
    {
        private Transform _transform;
        private Connection _connection;

        public ReaderConvertDataTypes(Connection connection, Transform transform)
        {
            _transform = transform;
            _connection = connection;
            CacheTable = _transform.CacheTable;
        }

        public override string Details()
        {
            return $"Conversion for connection: {_connection.Name}";
        }

        public override bool ResetTransform()
        {
            return _transform.ResetTransform();
        }

        protected async override Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if(await _transform.ReadAsync(cancellationToken) == false)
            {
                return null;
            }

            var row = new object[_transform.FieldCount];
            for(var i = 0; i < _transform.FieldCount; i++)
            {
                row[i] = _connection.ConvertForWrite(CacheTable.Columns[i], _transform[i]);
            }

            return row;
        }
    }
}
