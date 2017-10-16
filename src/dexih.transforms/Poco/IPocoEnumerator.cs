using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms.Poco
{
    public interface IPocoEnumerator<out T>: IEnumerator<T>
    {
        Task<bool> MoveNextAsync(CancellationToken cancellationToken);
        T this[int index] { get; }
        int Count { get; }
        Task<int> CountAsync(CancellationToken cancellationToken);
    }  
}