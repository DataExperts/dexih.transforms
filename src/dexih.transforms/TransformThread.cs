using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
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
            SetInTransform(transform, null, true);
//            PrimaryTransform = transform;
//            _transform = transform;
            _currentRow = 0;
        }

        private int _currentRow;
        
        public override string TransformName { get; } = "Transform Thread";
        public override string TransformDetails => "";


        public override Task<bool> Open(long auditKey, SelectQuery query = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;

            if (PrimaryTransform == null)
            {
                throw new TransformException("Open failed, there is no primary transform set fo the transform thread.");
            }

            if (!PrimaryTransform.IsOpen)
            {
                return PrimaryTransform.Open(auditKey, query, cancellationToken);
            }

            return Task.FromResult(true);
        }
        
 
        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            return PrimaryTransform.ReadThreadSafe(_currentRow++, cancellationToken);
        }

        public override bool ResetTransform()
        {
            _currentRow = 0;
            return true;
        }

        public override Table CacheTable => PrimaryTransform.CacheTable;
    }
}