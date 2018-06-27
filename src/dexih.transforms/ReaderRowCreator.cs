using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms
{
    /// <summary>
    /// Generates a sequence of rows starting at "StartAt", ending at "EndAt", with increment of "Increment"
    /// </summary>
    public class ReaderRowCreator : Transform
    {
        private int? _currentRow;

        public int StartAt { get; set; }
        public int EndAt { get; set; }
        public int Increment { get; set; }


        private bool _doLookup = false;
        private int? _lookupRow = null;

        public Table GetTable()
        {
            var table = new Table("RowCreator");
            table.Columns.Add(new TableColumn("RowNumber", ETypeCode.Int32) { IsMandatory = true});
            table.OutputSortFields = SortFields;
            return table;
        }
        
        public void InitializeRowCreator(int startAt, int endAt, int increment)
        {
            StartAt = startAt;
            EndAt = endAt;
            Increment = increment;

            if (Increment == 0)
            {
                throw new ArgumentOutOfRangeException(nameof(increment));
            }

            InitializeOutputFields();
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = GetTable();
            _currentRow = null;
            return true;
        }

        public override bool RequiresSort => false;

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken)
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

        public override string Details()
        {
            return $"RowCreator: Starts at: {StartAt} , Ends At: {EndAt}, Increments by {Increment}";
        }

        public override List<Sort> RequiredSortFields()
        {
            return null;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

        public override List<Sort> SortFields => new List<Sort>
        {
            new Sort(new TableColumn("RowNumber", ETypeCode.Int32), Increment > 0 ? Sort.EDirection.Ascending : Sort.EDirection.Descending)
        };

        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;
            Reset();
            
            var filter = query.Filters.FirstOrDefault(c => c.Column1?.Name == "RowNumber");
            if (filter != null)
            {
                 _lookupRow = Convert.ToInt32(filter.Value2 ?? 0);
            }
            else
            {
                _lookupRow = StartAt;
            }

            _doLookup = true;
            return Task.FromResult(true);
        }

    }
}
