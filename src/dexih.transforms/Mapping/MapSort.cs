using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;

namespace dexih.transforms.Mapping
{
    public class MapSort: Mapping
    {
       
        public MapSort(TableColumn inputColumn, ESortDirection sortDirection)
        {
            InputColumn = inputColumn;
            SortDirection = sortDirection;
        }
        
        public ESortDirection SortDirection;
        public object InputValue;
        public TableColumn InputColumn;

        private int _inputOrdinal = -1;

        private object[] _rowData;

        public override void InitializeColumns(Table table, Table joinTable = null, Mappings mappings = null)
        {
            if (InputColumn != null)
            {
                _inputOrdinal = table.GetOrdinal(InputColumn);
                if (_inputOrdinal < 0 && InputValue == null)
                {
                    InputValue = InputColumn.DefaultValue;
                }
            }
        }

        public override void AddOutputColumns(Table table)
        {
        }

        public override Task<bool> ProcessInputRowAsync(FunctionVariables functionVariables, object[] row, object[] joinRow = null, CancellationToken cancellationToken = default)
        {
            _rowData = row;
            return Task.FromResult(true);
        }

        public override void MapOutputRow(object[] data) 
        {
        }

        public override object GetOutputValue(object[] row = null)
        {
            if (_inputOrdinal == -1)
            {
                return InputValue;
            }
            else
            {
                return row == null ? row[_inputOrdinal] : _rowData[_inputOrdinal];     
            }        
        }

        public override string Description()
        {
            return $"Sort {InputColumn?.Name} {SortDirection}";
        }

        public override void Reset(EFunctionType functionType)
        {
            _rowData = null;
        }
        
        // public override IEnumerable<SelectColumn> GetRequiredColumns(bool includeAggregate)
        // {
        //     if (InputColumn == null)
        //     {
        //         return new SelectColumn[0];
        //     }
        //
        //     return new[] {new SelectColumn(InputColumn)};
        // }

        /// <summary>
        /// Note, with the sort extra checked need to be made to ensure sorts are in correct order.
        /// </summary>
        /// <param name="selectQuery"></param>
        /// <returns></returns>
        public override bool MatchesSelectQuery(SelectQuery selectQuery)
        {
            if(selectQuery.Sorts == null || 
               !selectQuery.Sorts.Any() ||
               InputColumn == null)
            {
                return false;
            }

            foreach (var sortColumn in selectQuery.Sorts)
            {
                if (sortColumn.Column.Name == InputColumn.Name && sortColumn.Direction == SortDirection)
                {
                    return true;
                }
            }

            return false;
        }
    }
}