using System.Collections.Generic;
using System.Linq;

namespace dexih.functions.Query
{
    public class SelectQuery
    {
        public SelectQuery()
        {
            Columns = new List<SelectColumn>();
            Filters = new List<Filter>();
            Sorts = new List<Sort>();
            Groups = new List<TableColumn>();
            Rows = -1; //-1 means show all rows.
        }

        public List<SelectColumn> Columns { get; set; }
        public string Table { get; set; }
        public List<Filter> Filters { get; set; }
        public List<Sort> Sorts { get; set; }
        public List<TableColumn> Groups { get; set; }
        public int Rows { get; set; }
        
        /// <summary>
        /// Used for flatfiles to specify only a specific filename
        /// </summary>
        public string FileName { get; set; }
        public EFlatFilePath Path { get; set; }

        /// <summary>
        /// Tests is a row should be filtered based on the filters provided.  
        /// </summary>
        /// <param name="row"></param>
        /// <param name="filters"></param>
        /// <param name="table"></param>
        /// <returns>true = don't filter, false = filtered</returns>
        public bool EvaluateRowFilter(IReadOnlyList<object> row, Table table)
        {
            if (Filters != null && Filters.Count > 0)
            {
                var filterResult = true;
                var isFirst = true;

                foreach (var filter in Filters)
                {
                    var column1Value = filter.Column1 == null
                        ? filter.Value1
                        : row[table.GetOrdinal(filter.Column1.Name)];
                    
                    var column2Value = filter.Column2 == null
                        ? filter.Value2
                        : row[table.GetOrdinal(filter.Column2.Name)];

                    if (isFirst)
                    {
                        filterResult = filter.Evaluate(column1Value, column2Value);
                        isFirst = false;
                    }
                    else if (filter.AndOr == Filter.EAndOr.And)
                    {
                        filterResult = filterResult && filter.Evaluate(column1Value, column2Value);
                    }
                    else
                    {
                        filterResult = filterResult || filter.Evaluate(column1Value, column2Value);
                    }
                }

                return filterResult;
            }
            else
            {
                return true;
            }
        }

    }
}