using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms.tests
{
    public class MockReader: Transform
    {
        public MockReader(Table table)
        {
            CacheTable = table;
        }
        public override string TransformName { get; }
        public override Dictionary<string, object> TransformProperties()
        {
            throw new System.NotImplementedException();
        }
        
        public override Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            SelectQuery = requestQuery;
            return Task.FromResult(true);
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            throw new System.NotImplementedException();
        }

        public override bool ResetTransform()
        {
            return true;
        }
    }
}