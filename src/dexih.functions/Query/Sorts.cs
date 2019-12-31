using MessagePack;
using System.Collections.Generic;

namespace dexih.functions.Query
{
    [MessagePackObject]
    public class Sorts : List<Sort>
    {
        public Sorts() {}

        public Sorts(IEnumerable<Sort> sorts)
        {
            if (sorts == null) return;

            AddRange(sorts);
        }
        
        public Sorts(params string[] columnNames)
        {
            foreach (var columnName in columnNames)
            {
                Add(new Sort(columnName));
            }
        }

        public Sorts(params (string columnName, ESortDirection direction)[] sorts)
        {
            foreach (var sort in sorts)
            {
                Add(new Sort(sort.columnName, sort.direction));
            }
        }

        public void Add(string name, ESortDirection sortDirection)
        {
            base.Add(new Sort(name, sortDirection));
        }

        public void Add(TableColumn column, ESortDirection sortDirection)
        {
            base.Add(new Sort(column, sortDirection));
        }

    }
}