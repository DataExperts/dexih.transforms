using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using dexih.connections.sql;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms.Mapping;
using dexih.transforms.Transforms;
using Dexih.Utils.CopyProperties;

namespace dexih.transforms
{
    [Transform(
        Name = "Storage Cache",
        Description = "Caches data to storage before passing on.  Allows sorts/groups/joins to pushdown.",
        TransformType = ETransformType.Internal
    )]
    public class TransformStorageCache : Transform
    {
        public ConnectionSql ConnectionSql { get; set; }

        private Transform _cachePrimary;
        private Transform _cacheReference;
        private Table _tablePrimary;
        private Table _tableReference;

        private bool _firstRead = true;
        
        public TransformStorageCache()
        {
        }

        public TransformStorageCache(Transform inTransform, ConnectionSql connectionSql)
        {
            ConnectionSql = connectionSql;
            SetInTransform(inTransform);
        }
        
        public override bool RequiresSort => false;
        
        public override string TransformName { get; } = "Storage Cache";

        public override Dictionary<string, object> TransformProperties()
        {
            return _cachePrimary?.TransformProperties();
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            IsOpen = true;
            _firstRead = true;

            var unique = ShortGuid.NewGuid().ToString().Replace("-", "");
            _tablePrimary = PrimaryTransform.CacheTable.Copy(true);
            _tablePrimary.Name = "primary-" + unique;

            var newSelectQuery = requestQuery?.CloneProperties();
            
            if (newSelectQuery != null)
            {
                // reset the delta types to ensure unnecessary indexes are not created.
                foreach (var column in _tablePrimary.Columns)
                {
                    column.DeltaType = EDeltaType.TrackingField;
                }
                _tablePrimary.Columns.RebuildOrdinals();

                // make sort columns an index
                if (newSelectQuery.Groups?.Count > 0)
                {
                    var index = new TableIndex()
                    {
                        Name = "index_group_" + unique,
                        Columns = newSelectQuery.Groups.Select(c => new TableIndexColumn(c.Name))
                            .ToList()
                    };
                    _tablePrimary.Indexes.Add(index);
                } 
                else if (newSelectQuery.Sorts?.Count > 0)
                {
                    var index = new TableIndex()
                    {
                        Name = "index_sort_" + unique,
                        Columns = newSelectQuery.Sorts.Select(c => new TableIndexColumn(c.Column.Name, c.Direction))
                            .ToList()
                    };
                    _tablePrimary.Indexes.Add(index);
                }
                // TODO Add joins to the created indexes.

            }

            SetRequestQuery(newSelectQuery, true);

            await ConnectionSql.CreateTable(_tablePrimary, true, cancellationToken);

            _cachePrimary = ConnectionSql.GetTransformReader(_tablePrimary);
            _cachePrimary.TableAlias = PrimaryTransform.TableAlias;
            _cachePrimary.Open(newSelectQuery, cancellationToken);
            GeneratedQuery = _cachePrimary.GeneratedQuery;
            CacheTable = _cachePrimary.CacheTable.Copy();
            CacheTable.OutputSortFields = GeneratedQuery.Sorts;
            
            if (ReferenceTransform != null)
            {
                _tableReference = ReferenceTransform.CacheTable.Copy(true);
                _tableReference.Name = "reference-" + (new ShortGuid());
                await ConnectionSql.CreateTable(_tablePrimary, true, cancellationToken);
            }
            
            var returnValue = await PrimaryTransform.Open(auditKey, requestQuery, cancellationToken);
            
            return returnValue;
        }

        protected override SelectQuery GetGeneratedQuery(SelectQuery requestQuery)
        {
            var generatedQuery = new SelectQuery()
            {
                Columns = _cachePrimary.Columns,
                Sorts = _cachePrimary.SortFields,
                Filters = PrimaryTransform.Filters,
                Joins = PrimaryTransform.Joins,
                Groups = _cachePrimary.Groups,
                GroupFilters = _cachePrimary.GroupFilters,
                Alias = PrimaryTransform.TableAlias
            };

            return generatedQuery;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            // for the first read, load the cache table in full
            if(_firstRead)
            {
                _firstRead = false;
                await ConnectionSql.ExecuteInsertBulk(_tablePrimary, PrimaryTransform, cancellationToken);
            }

            if (await _cachePrimary.ReadAsync(cancellationToken))
            {
                var values = new object[PrimaryTransform.FieldCount];
                _cachePrimary.GetValues(values);
                return values;
            }
            else
            {
                await _cachePrimary.CloseAsync();
                await ConnectionSql.DropTable(_tablePrimary, cancellationToken);
                return null;
            }
        }
        
        protected override async Task CloseConnections()
        {
            if (_cachePrimary.IsOpen)
            {
                await _cachePrimary.CloseAsync();
                await ConnectionSql.DropTable(_tablePrimary, CancellationToken.None);
            }
        }
        
        public override bool ResetTransform()
        {
            _firstRead = true;
            return true;
        }
        
        public override Sorts RequiredSortFields()
        {
            return _cachePrimary.SortFields;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }
        
    }



}
