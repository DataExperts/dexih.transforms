using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using Newtonsoft.Json.Linq;


namespace dexih.transforms
{
    /// <summary>
    /// Simple json reader, that takes a jArray and maps to the input table.
    /// </summary>
    public class ReaderJson : Transform
    {
        private JArray _jArray;
        private int _rowNumber;


        public ReaderJson(JArray jArray, Table table)
        {
            _jArray = jArray;
            CacheTable = table;
        }
        
        public override string TransformName { get; } = "Json Reader";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }
        
        protected override void CloseConnections()
        {

        }

        public override Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;
            return Task.FromResult(true);
        }

        public override bool ResetTransform()
        {
            return IsOpen;
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (_rowNumber >= _jArray.Count)
            {
                return null; 
            }
            
            var row = new object[CacheTable.Columns.Count];
            var jToken = _jArray[_rowNumber++];

            for (var i = 0; i < CacheTable.Columns.Count; i++)
            {
                row[i] = jToken[CacheTable.Columns[i].Name];
            }

            return Task.FromResult(row);
        }
        
    }
}