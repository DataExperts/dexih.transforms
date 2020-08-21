using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.DataType;

namespace dexih.transforms.Mapping
{
    public class MapFilter: Mapping
    {
        public MapFilter() {}
        
        public MapFilter(TableColumn column1, TableColumn column2, ECompare @operator = ECompare.IsEqual)
        {
            Column1 = column1;
            Column2 = column2;
            Operator = @operator;
        }

        public MapFilter(TableColumn column1, object value2, ECompare @operator = ECompare.IsEqual)
        {
            Column1 = column1;
            Value2 = value2;
            Operator = @operator;
        }

        public TableColumn Column1 { get; set; }
        public TableColumn Column2 { get; set; }
        public object Value1 { get; set; }
        public object Value2 { get; set; }
        
        public ECompare Operator { get; set; } = ECompare.IsEqual;

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
            var dataType = Column1?.DataType ?? Column2.DataType;

            var returnValue = Operations.Evaluate(Operator, dataType, value1, value2);
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
            return $"Filter({item1} {Operator} {item2}";
        }


        public MapFilter Copy()
        {
            var filter = new MapFilter()
            {
                Column1 = Column1,
                Column2 = Column2,
                Value1 = Value1,
                Value2 = Value2,
                Operator = Operator
            };

            return filter;
        }
        
        public override IEnumerable<SelectColumn> GetRequiredColumns(bool includeAggregate)
        {
            if (Column1 != null) { yield return new SelectColumn(Column1);}
            if (Column2 != null) { yield return new SelectColumn(Column2);}
        }

        public override bool MatchesSelectQuery(SelectQuery selectQuery)
        {
            if(selectQuery.Filters == null || 
               !selectQuery.Filters.Any())
            {
                return false;
            }

            foreach (var filter in selectQuery.Filters)
            {
                if (filter.Column1?.Name == Column1?.Name && filter.Operator == Operator )
                {
                    if(filter.Column2 == null && Column2 == null && Value2 == filter.Value2) return true;
                    if(filter.Column2 != null && Column2 != null && filter.Column2.Name == Column2.Name) return true;
                }
                if (filter.Column2?.Name == Column2?.Name && filter.Operator == Operator )
                {
                    if(filter.Column1 == null && Column1 == null && Value1 == filter.Value1) return true;
                    if(filter.Column1 != null && Column1 != null && filter.Column1.Name == Column1.Name) return true;
                }
            }

            return false;
        }
    }
}