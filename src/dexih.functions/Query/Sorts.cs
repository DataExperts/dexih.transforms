using System.Collections.Generic;

namespace dexih.functions.Query
{
    public class Sorts: List<Sort>
    {
        public Sorts(params string[] columnNames)
        {
            foreach (var columnName in columnNames)
            {
                Add(new Sort(columnName));
            }
        }
        
        public Sorts(params (string columnName, Sort.EDirection direction)[] sorts)
        {
            foreach (var sort in sorts)
            {
                Add(new Sort(sort.columnName, sort.direction));
            }
        }
        
    }
}