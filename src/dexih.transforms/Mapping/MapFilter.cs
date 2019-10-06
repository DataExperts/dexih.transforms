using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using Dexih.Utils.DataType;

namespace dexih.transforms.Mapping
{
    public class MapFilter: Mapping
    {
        public MapFilter() {}
        
        public MapFilter(TableColumn column1, TableColumn column2, ECompare compare = ECompare.IsEqual)
        {
            Column1 = column1;
            Column2 = column2;
            Compare = compare;
        }

        public MapFilter(TableColumn column1, object value2, ECompare compare = ECompare.IsEqual)
        {
            Column1 = column1;
            Value2 = value2;
            Compare = compare;
        }

        public TableColumn Column1 { get; set; }
        public TableColumn Column2 { get; set; }
        public object Value1 { get; set; }
        public object Value2 { get; set; }
        
        public ECompare Compare { get; set; } = ECompare.IsEqual;

        private int _column1Ordinal = -1;
        private int _column2Ordinal = -1;

        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
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
        }

        public override Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            var value1 = _column1Ordinal == -1 ? Value1 : row[_column1Ordinal];
            var value2 = _column2Ordinal == -1 ? Value2 : row[_column2Ordinal];

            var returnValue = Operations.Evaluate(Compare, Column1.DataType, value1, value2);
            return Task.FromResult(returnValue);
        }

        public override void MapOutputRow(object[] row)
        {
        }

        public override object GetOutputValue(object[] row = null)
        {
            throw new NotSupportedException();
        }

        public override string Description()
        {
            var item1 = _column1Ordinal == -1 ? Value1 : Column1.Name;
            var item2 = _column2Ordinal == -1 ? Value2 : Column2.Name;
            return $"Filter({item1} {Compare} {item2}";
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
        
        public override IEnumerable<TableColumn> GetRequiredColumns()
        {
            var columns = new List<TableColumn>();
            if(Column1 != null) { columns.Add(Column1);}
            if(Column2 != null) { columns.Add(Column2);}
            return columns;
        }

    }
}