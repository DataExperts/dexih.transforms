using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Query;

namespace dexih.transforms
{
    /// <summary>
    /// Transform which caches all data from the inTransform.
    /// </summary>
    public class TransformCache: Transform
    {
        private Transform _transform;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="inTransform"></param>
        public TransformCache(Transform inTransform)
        {
            _transform = inTransform;
            CacheTable = _transform.CacheTable.Copy();
        }

        public override bool IsClosed => _transform.IsClosed;
        public override ECacheMethod CacheMethod { get; protected set; } = ECacheMethod.DemandCache;

        public override string TransformName { get; } = "Reader Cache";
        public override string TransformDetails => CacheTable?.Name ?? "Unknown";

        public override bool ResetTransform()
        {
            IsReaderFinished = false;
            SetRowNumber(0);
            return true;
        }
        
        public override async Task<bool> Open(long auditKey, SelectQuery selectQuery = null, CancellationToken cancellationToken = default)
        {
            if (!IsOpen)
            {
                IsOpen = true;
                CacheTable = _transform.CacheTable.Copy();
                await _transform.Open(auditKey, selectQuery, cancellationToken);

                // load the cache.
                while (await _transform.ReadAsync(cancellationToken))
                {
                    CacheTable.Data.Add(_transform.CurrentRow);
                }
            }

            ResetTransform();

            return true;
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            // always return null, as this means the cache has been exhausted
            return Task.FromResult<object[]>(null);
        }
    }
}