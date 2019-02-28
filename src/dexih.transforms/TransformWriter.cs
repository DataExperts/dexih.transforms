using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms
{
    /// <summary>
    /// The base class for writing rows from a transform to target tables.
    /// </summary>
    public abstract class Writer
    {
        /// <summary>
        /// Writes all record from the inTransform to the target table and reject table.
        /// </summary>
        /// <param name="inTransform">Transform to read data from</param>
        /// <param name="writerTargets"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public abstract Task<bool> WriteRecordsAsync(Transform inTransform, TransformWriterTargets writerTargets, CancellationToken cancellationToken);
    }
}