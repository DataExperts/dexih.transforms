using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms
{

    /// <summary>
    /// This reader can be used to convert any data into datatypes supported by a target connection.
    /// </summary>
    public sealed class ReaderConvertDataTypes: Transform
    {
        private readonly Connection _connection;

        private readonly int _operationOrdinal;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connection">The target connection to standardise on</param>
        /// <param name="transform">The inbound transform</param>
        public ReaderConvertDataTypes(Connection connection, Transform transform)
        {
            PrimaryTransform = transform;
            _connection = connection;
            CacheTable = PrimaryTransform.CacheTable.Copy();

            // set all columns to allow nulls to ensure no errors occur.
            foreach (var col in CacheTable.Columns)
            {
                col.AllowDbNull = true;
            }
            
            _operationOrdinal = CacheTable.GetOrdinal(EDeltaType.DatabaseOperation);

            foreach (var column in CacheTable.Columns)
            {
                if (column.Rank > 0 && !connection.CanUseArray || column.DataType == ETypeCode.Node)
                {
                    column.Rank = 0;
                    column.DataType = ETypeCode.String;
                }
            }

            IsOpen = PrimaryTransform.IsOpen;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            await PrimaryTransform?.Open(auditKey, requestQuery, cancellationToken);
            IsOpen = PrimaryTransform.IsOpen;

            GeneratedQuery = PrimaryTransform.GeneratedQuery;
            
            return IsOpen;
        }

        public override string TransformName { get; } = "Data Type Converter";

        public override Dictionary<string, object> TransformProperties()
        {
            var connectionReference = _connection?.GetConnectionReference();

            if (connectionReference != null)
            {
                return new Dictionary<string, object>()
                {
                    {"ConnectionName", _connection.Name},
                    {"DatabaseName", connectionReference.Name},
                };
            }

            return null;
        }


        public override bool ResetTransform()
        {
            return PrimaryTransform.ResetTransform();
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if(await PrimaryTransform.ReadAsync(cancellationToken) == false)
            {
                return null;
            }

            if (_operationOrdinal >= 0)
            {
                if (Equals(PrimaryTransform[_operationOrdinal],'T'))
                {
                    return PrimaryTransform.CurrentRow;
                }
            }

            var row = new object[PrimaryTransform.FieldCount];
            for(var i = 0; i < PrimaryTransform.FieldCount; i++)
            {
                if (CacheTable.Columns[i].DeltaType == EDeltaType.DatabaseOperation)
                {
                    row[i] = PrimaryTransform[i];
                    continue;
                } 

                row[i] = _connection.ConvertForWrite(CacheTable.Columns[i], PrimaryTransform[i]).value;
            }

            return row;
        }
    }
}
