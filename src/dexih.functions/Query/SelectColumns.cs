using System.Collections.Generic;
using System.Linq;

namespace dexih.functions.Query
{
    public class SelectColumns: List<SelectColumn>
    {
        public SelectColumns(): base()
        {
            
        }

        public SelectColumns(IEnumerable<SelectColumn> selectColumns)
        {
            if (selectColumns == null) return;
            
            AddRange(selectColumns);    
        }

        public SelectColumns(params SelectColumn[] selectColumns): base(selectColumns?? new SelectColumn[0])
        {
            
        }

        public SelectColumns(IEnumerable<TableColumn> columns): base(columns.Select(c => new SelectColumn(c)))
        {
            
        }

        public SelectColumns(params TableColumn[] columns): base(columns.Select(c => new SelectColumn(c)))
        {
            
        }
        

        public void Add(TableColumn column, EAggregate aggregate = EAggregate.None, TableColumn outputColumn = null)
        {
            var selectColumn = new SelectColumn(column, aggregate, outputColumn);
        }
        
        public void Add(string columnName, EAggregate aggregate = EAggregate.None, string outputColumnName = null)
        {
            var selectColumn = new SelectColumn(columnName, aggregate, outputColumnName);
        }

    }
}