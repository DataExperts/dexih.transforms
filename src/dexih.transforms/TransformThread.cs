using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Query;
using dexih.transforms.Exceptions;

namespace dexih.transforms
{
    
    /// <summary>
    /// Allows the same transform to be read by multiple threads.
    /// </summary>
    public class TransformThread : Transform
    {
        public TransformThread(Transform transform)
        {
            PrimaryTransform = transform;
            CacheTable = PrimaryTransform.CacheTable;
            _currentRow = 0;
        }

        private int _currentRow;
        
        public override string TransformName { get; } = "Transform Thread";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;
            SelectQuery = selectQuery;

            if (PrimaryTransform == null)
            {
                throw new TransformException("Open failed, there is no primary transform set fo the transform thread.");
            }

            if (!PrimaryTransform.IsOpen && !PrimaryTransform.IsReaderFinished)
            {
                return PrimaryTransform.Open(auditKey, selectQuery, cancellationToken);
            }
            

            return Task.FromResult(true);
        }
        
 
        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            return PrimaryTransform.ReadThreadSafe(_currentRow++, cancellationToken);
        }

        public override bool ResetTransform()
        {
            _currentRow = 0;
            return true;
        }

    }
}