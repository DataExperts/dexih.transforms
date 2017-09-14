using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;

namespace dexih.transforms
{
    public class ReaderRowCreator : Transform
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
            CacheTable.Columns.Add(new TableColumn("RowNumber", ETypeCode.Int32));

            CacheTable.OutputSortFields = new List<Sort>() { new Sort(new TableColumn("RowNumber", ETypeCode.Int32)) };
            _currentRow = StartAt-1;
            return true;
        }

        public override bool RequiresSort => false;

        protected override Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {

            _currentRow = _currentRow + Increment;
            if (_currentRow > EndAt)
                return Task.FromResult<object[]>(null);

            var newRow = new object[] { _currentRow };
            return Task.FromResult(newRow);
        }

        public override bool ResetTransform()
        {
            _currentRow = StartAt-1;
            return true;
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

        public override List<Sort> SortFields
        {
            get
            {
                return new List<Sort>() { new Sort(new TableColumn("RowNumber", ETypeCode.Int32)) };
            }
        }

    }
}
