using System.Collections.Generic;
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
        private readonly Transform _transform;

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
        public override ECacheMethod CacheMethod { get; protected set; } = ECacheMethod.NoCache;

        public override string TransformName { get; } = "Reader Cache";

        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override bool ResetTransform()
        {
            IsReaderFinished = false;
            SetRowNumber(0);
            return true;
        }
        
        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            SelectQuery = requestQuery;
            
            if (!IsOpen)
            {
                IsOpen = true;
                CacheTable = _transform.CacheTable.Copy();
                await _transform.Open(auditKey, requestQuery, cancellationToken);

                GeneratedQuery = _transform.GeneratedQuery;
                
                // load the cache.
                while (await _transform.ReadAsync(cancellationToken))
                {
                    CacheTable.AddRow(_transform.CurrentRow);
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