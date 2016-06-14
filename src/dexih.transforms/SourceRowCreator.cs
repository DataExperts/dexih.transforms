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

            InitializeOutputFields();
        }

        public override bool InitializeOutputFields()
        {
            CacheTable = new Table("RowCreator");
            CacheTable.Columns.Add(new TableColumn("RowNumber", DataType.ETypeCode.Int32));

            CacheTable.OutputSortFields = new List<Sort>() { new Sort("RowNumber") };
            _currentRow = StartAt-1;
            return true;
        }

        public override bool RequiresSort => false;

        protected override ReturnValue<object[]> ReadRecord()
        {
            _currentRow = _currentRow + Increment;
            if (_currentRow > EndAt)
                return new ReturnValue<object[]>(false, null);
            var newRow = new object[] { _currentRow };
            return new ReturnValue<object[]>(true, newRow);
        }

        public override ReturnValue ResetTransform()
        {
            _currentRow = StartAt-1;
            return new ReturnValue(true);
        }

        public override string Details()
        {
            return "RowCreator: Starts at: " + StartAt + ", Ends At: " + EndAt;
        }

        public override List<Sort> RequiredSortFields()
        {
            return null;
        }

        public override List<Sort> RequiredReferenceSortFields()
        {
            return null;
        }

    }
}
