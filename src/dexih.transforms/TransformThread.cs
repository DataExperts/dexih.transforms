using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    
    /// <summary>
    /// Allows the same transform to be read by multiple threads.
    /// </summary>
    public class TransformThread : Transform
    {
        public TransformThread(Transform transform)
        {
            PrimaryTransform = transform.PrimaryTransform;
            _transform = transform;
            _currentRow = 0;
        }

        private readonly Transform _transform;
        private int _currentRow;

        public override string Details()
        {
            return $"Transform Thread";
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            return _transform.ReadThreadSafe(_currentRow++, cancellationToken);
        }

        public override bool ResetTransform()
        {
            _currentRow = 0;
            return true;
        }

        public override Table CacheTable => _transform.CacheTable;
    }
}