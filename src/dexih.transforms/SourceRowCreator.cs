using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading.Tasks;
using dexih.functions;

namespace dexih.transforms
{
    public class SourceRowCreator : Transform
    {
        private int _currentRow;

        public int StartAt { get; set; }
        public int EndAt { get; set; }
        public int Increment { get; set; }

        public void InitializeRowCreator(int startAt, int endAt, int increment)
        {
            StartAt = startAt;
            EndAt = endAt;
            Increment = increment;

            Initialize();
        }

        public override bool Initialize()
        {
            CachedTable = new Table("RowCreator");
            CachedTable.Columns.Add(new TableColumn("RowNumber", DataType.ETypeCode.Int32));

            CachedTable.OutputSortFields = new List<Sort>() { new Sort("RowNumber") };
            _currentRow = StartAt-1;
            return true;
        }

        public override bool CanRunQueries => false;

        public override bool PrefersSort => false;
        public override bool RequiresSort => false;


        protected override bool ReadRecord()
        {
            _currentRow = _currentRow + Increment;
            if (_currentRow > EndAt)
                return false;
            CurrentRow = new object[] { _currentRow };
            return true;
        }

        public override bool ResetValues()
        {
            _currentRow = StartAt-1;
            return true; // not applicable for filter.
        }

        public override string Details()
        {
            return "RowCreator: Starts at: " + StartAt + ", Ends At: " + EndAt;
        }

        public override List<Sort> RequiredSortFields()
        {
            return null;
        }

        public override List<Sort> RequiredJoinSortFields()
        {
            return null;
        }

    }
}
