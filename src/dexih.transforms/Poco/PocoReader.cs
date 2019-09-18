using dexih.functions.Query;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace dexih.transforms.Poco
{
    /// <summary>
    /// A source transform that uses a prepopulated Table as an input.
    /// </summary>
    public class PocoReader<T> : Transform
    {
        private readonly IEnumerator<T> _enumerator;

        private readonly PocoTable<T> _pocoTable;

        public override ECacheMethod CacheMethod
        {
            get => ECacheMethod.DemandCache;
            protected set => throw new Exception("Cache method is always Demand in the Poco Reader and cannot be set.");
        }

        #region Constructors
        public PocoReader(PocoTable<T> pocoTable, IEnumerable<T> items)
        {
            _enumerator = items.GetEnumerator();
            _pocoTable = pocoTable;
            CacheTable = _pocoTable.Table;
            Reset();
        }

        public PocoReader(IEnumerable<T> items)
        {
            _enumerator = items.GetEnumerator();
            _pocoTable = new PocoTable<T>();
            CacheTable = _pocoTable.Table;
            Reset();
        }

        public override Sorts SortFields { get => _pocoTable?.Table?.OutputSortFields; }


        #endregion

        public override string TransformName { get; } = "Poco Reader";

        public override Dictionary<string, object> TransformProperties() =>
            new Dictionary<string, object>()
            {
                {"TableName", _pocoTable?.Table.Name ?? "Unknown"}
            };

        


        public override bool ResetTransform()
        {
            CurrentRowNumber = -1;
            return true;
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (_enumerator.MoveNext())
            {
                var item = _enumerator.Current;
                var row = new object[_pocoTable.TableMappings.Count];
                foreach (var mapping in _pocoTable.TableMappings)
                {
                    row[mapping.Position] = mapping.PropertyInfo.GetValue(item);
                }

                return Task.FromResult(row);
            }
            else
            {
                return Task.FromResult<object[]>(null);
            }
        }
    }
}
