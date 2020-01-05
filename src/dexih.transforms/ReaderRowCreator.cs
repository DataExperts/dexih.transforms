using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms
{

    /// <summary>
    /// Generates a sequence of rows starting at "StartAt", ending at "EndAt", with increment of "Increment"
    /// </summary>
    /// 
    public class ReaderRowCreator : Transform
    {
        private int? _currentRow;

        public int StartAt { get; set; }
        public int EndAt { get; set; }
        public int Increment { get; set; }


        private bool _doLookup = false;
        private int? _lookupRow = null;

        public override string TransformName { get; } = "Row Generator";

        public override Dictionary<string, object> TransformProperties()
        {
            if (CacheTable != null)
            {
                return new Dictionary<string, object>()
                {
                    {"StartAt", StartAt},
                    {"EndAt", EndAt},
                    {"Increment", Increment},
                };
            }

            return null;
        }
        
        public Table GetTable()
        {
            var table = new Table("RowCreator");
            var column = new TableColumn("RowNumber", ETypeCode.Int32, EDeltaType.NaturalKey) {IsMandatory = true};
            table.Columns.Add(column);
            table.OutputSortFields = new Sorts
            {
                new Sort(column,
                    Increment > 0 ? ESortDirection.Ascending : ESortDirection.Descending)
            };
            return table;
        }
        
        public void InitializeRowCreator(int startAt, int endAt, int increment)
        {
            IsOpen = true;
            
            StartAt = startAt;
            EndAt = endAt;
            Increment = increment;

            if (Increment == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(increment));
            }

            CacheTable = GetTable();
            _currentRow = null;

        }

        public override bool RequiresSort => false;

        public override Task<bool> Open(long auditKey, SelectQuery requestQuery = null,
            CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            SelectQuery = requestQuery;
            
            GeneratedQuery = new SelectQuery()
            {
                Sorts = CacheTable.OutputSortFields
            };

            return Task.FromResult(true);
        }

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (_doLookup)
            {
                if(_lookupRow == null)
                    return Task.FromResult<object[]>(null);
                else
                {
                    var lookupRow =new object[] {_lookupRow};
                    _lookupRow = null;
                    return Task.FromResult(lookupRow);
                }
            }

            _currentRow = _currentRow == null ? StartAt : _currentRow + Increment;
            if (_currentRow > EndAt)
            {
                return Task.FromResult<object[]>(null);
            }

            var newRow = new object[] { _currentRow };
            return Task.FromResult(newRow);
        }

        public override bool ResetTransform()
        {
            _currentRow = null;
            return true;
        }

        public override Sorts RequiredSortFields()
        {
            return null;
        }

        public override Sorts RequiredReferenceSortFields()
        {
            return null;
        }

        public override async Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;
            await Open(auditKey, query, cancellationToken);
            Reset();
            
            var filter = query.Filters.FirstOrDefault(c => c.Column1?.Name == "RowNumber");
            _lookupRow = filter != null ? Convert.ToInt32(filter.Value2 ?? 0) : StartAt;

            _doLookup = true;
            return true;
        }

    }
}
