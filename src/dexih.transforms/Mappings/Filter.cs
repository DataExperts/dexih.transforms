using dexih.functions;
using dexih.functions.Query;
using System;
using System.Collections.Generic;
using System.Text;

namespace dexih.transforms
{
    public class FilterPair
    {
        public FilterPair()
        {
        }

        public FilterPair(TableColumn column1, TableColumn column2, Filter.ECompare compare = Filter.ECompare.IsEqual)
        {
            Column1 = column1;
            Column2 = column2;
            FilterValue = null;
            Compare = compare;
        }

        public FilterPair(TableColumn column1, object filterValue, Filter.ECompare compare = Filter.ECompare.IsEqual)
        {
            Column1 = column1;
            Column2 = null;
            FilterValue = filterValue;
            Compare = compare;
        }

        public TableColumn Column1 { get; set; }
        public TableColumn Column2 { get; set; }
        public object FilterValue { get; set; }
        public Filter.ECompare Compare { get; set; }
    }
}
