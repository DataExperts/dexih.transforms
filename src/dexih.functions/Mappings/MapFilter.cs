using System;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.functions.Mappings
{
    public class MapFilter: Mapping
    {
        public MapFilter() {}
        
        public MapFilter(TableColumn column1, TableColumn column2, Filter.ECompare compare = Filter.ECompare.IsEqual)
        {
            Column1 = column1;
            Column2 = column2;
            Compare = compare;
        }

        public MapFilter(TableColumn column1, Object value2, Filter.ECompare compare = Filter.ECompare.IsEqual)
        {
            Column1 = column1;
            Value2 = value2;
            Compare = compare;
        }

        public TableColumn Column1 { get; set; }
        public TableColumn Column2 { get; set; }
        public Object Value1 { get; set; }
        public Object Value2 { get; set; }
        
        public Filter.ECompare Compare { get; set; }

        private int _column1Ordinal = -1;
        private int _column2Ordinal = -1;

        public override void InitializeInputOrdinals(Table table, Table joinTable = null)
        {
            if (Column1 != null)
            {
                _column1Ordinal = table.GetOrdinal(Column1);
                if (_column1Ordinal < 0 && Value1 == null)
                {
                    Value1 = Column1.DefaultValue;
                }
            }
            else
            {
                _column1Ordinal = -1;
            }

            if (Column2 != null)
            {
                _column2Ordinal = table.GetOrdinal(Column2);
                if (_column2Ordinal < 0 && Value2 == null)
                {
                    Value2 = Column2.DefaultValue;
                }
            }
            else
            {
                _column2Ordinal = -1;
            }
        }

        public override void AddOutputColumns(Table table)
        {
            return;
        }

        public override bool ProcessInputRow(object[] row, object[] joinRow = null)
        {
            var value1 = _column1Ordinal == -1 ? Value1 : row[_column1Ordinal];
            var value2 = _column2Ordinal == -1 ? Value2 : row[_column2Ordinal];
            
            var compare = DataType.Compare(Column1.DataType, value1, value2);

            switch (Compare)
            {
                case Filter.ECompare.GreaterThan:
                    if (compare != DataType.ECompareResult.Greater)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.IsEqual:
                    if (compare != DataType.ECompareResult.Equal)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.GreaterThanEqual:
                    if (compare == DataType.ECompareResult.Less)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.LessThan:
                    if (compare != DataType.ECompareResult.Less)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.LessThanEqual:
                    if (compare == DataType.ECompareResult.Greater)
                    {
                        return false;
                    }
                    break;
                case Filter.ECompare.NotEqual:
                    if (compare == DataType.ECompareResult.Equal)
                    {
                        return false;
                    }
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }

            return true;
        }

        public override void ProcessOutputRow(object[] row)
        {
            return;
        }

        public override void ProcessResultRow(int index, object[] row)
        {
            return;
        }

        public override object GetInputValue(object[] row = null)
        {
            throw new NotSupportedException();
        }
        
      
        public override void Reset()
        {
            return;
        }

        public MapFilter Copy()
        {
            var filter = new MapFilter()
            {
                Column1 = Column1,
                Column2 = Column2,
                Value1 = Value1,
                Value2 = Value2,
                Compare = Compare
            };

            return filter;
        }
    }
}