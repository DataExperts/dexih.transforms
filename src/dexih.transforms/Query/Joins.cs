using System.Collections.Generic;

namespace dexih.functions.Query
{
 public class Joins: List<Join>
    {
        public Joins() {}

        public Joins(IEnumerable<Join> joins)
        {
            if (joins == null) return;

            base.AddRange(joins);
        }

        public Joins(EJoinType joinType, Table table, Filters joinFilters)
        {
            Add(new Join(joinType, table, joinFilters));
        }

        public Joins(EJoinType joinType, Table table, string alias, Filters joinFilters)
        {
            Add(new Join(joinType, table, alias, joinFilters));
        }
    }
}