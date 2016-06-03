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
            _currentRow = StartAt-1;
            return true;
        }

        public override int FieldCount => 1;

        public override bool CanRunQueries => false;

        public override bool PrefersSort => false;
        public override bool RequiresSort => false;


        public override string GetName(int i)
        {
            if (i == 0)
                return "RowNumber";
            throw new Exception("There is only one column available in the rowcreator transform");
        }

        public override int GetOrdinal(string columnName)
        {
            if (columnName == "RowNumber")
                return 0;
            throw new Exception("There is only one column with the name RowNumber in rowcreator transform");
        }

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

        //Always sorted by the RowNumber field.
        public override List<Sort> OutputSortFields()
        {
            return new List<Sort>() { new Sort("RowNumber") };
        }

    }
}
