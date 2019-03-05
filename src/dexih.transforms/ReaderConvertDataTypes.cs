using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{

    /// <summary>
    /// This reader can be used to convert any data into datatypes supported by a target connection.
    /// </summary>
    public sealed class ReaderConvertDataTypes: Transform
    {
        private Transform _transform;
        private Connection _connection;

        private int _operationOrdinal;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection">The target connection to standardise on</param>
        /// <param name="transform">The inbound transform</param>
        public ReaderConvertDataTypes(Connection connection, Transform transform)
        {
            _transform = transform;
            _connection = connection;
            CacheTable = _transform.CacheTable;

            _operationOrdinal = CacheTable.GetDeltaColumnOrdinal(TableColumn.EDeltaType.DatabaseOperation);
        }

        public override bool IsClosed => _transform.IsClosed;

        public override string TransformName { get; } = "Data Type Converter";
        public override string TransformDetails => CacheTable?.Name ?? "Unknown";

        public override bool ResetTransform()
        {
            return _transform.ResetTransform();
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            if(await _transform.ReadAsync(cancellationToken) == false)
            {
                return null;
            }

            if (_operationOrdinal >= 0)
            {
                if (object.Equals(_transform[_operationOrdinal],'T'))
                {
                    return _transform.CurrentRow;
                }
            }

            var row = new object[_transform.FieldCount];
            for(var i = 0; i < _transform.FieldCount; i++)
            {
                if (CacheTable.Columns[i].DeltaType == TableColumn.EDeltaType.DatabaseOperation)
                {
                    row[i] = _transform[i];
                    continue;
                } 

                row[i] = _connection.ConvertForWrite(CacheTable.Columns[i], _transform[i]);
            }

            return row;
        }
    }
}
