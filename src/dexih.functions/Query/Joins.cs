// using System.Collections;
// using System.Collections.Generic;
// using Dexih.Utils.DataType;
//
// namespace dexih.functions.Query
// {
//  public class Joins: List<Join>
//     {
//         public Joins() {}
//
//         public Joins(IEnumerable<Join> joins)
//         {
//             if (joins == null) return;
//
//             base.AddRange(joins);
//         }
//
//         public Joins(EJoinType joinType, string tableName, JoinFilters joinFilters)
//         {
//             Add(new Join(joinType, tableName, joinFilters));
//         }
//
//         public Joins(EJoinType joinType, string tableName, string alias, JoinFilters joinFilters)
//         {
//             Add(new Join(joinType, tableName, alias, joinFilters));
//         }
//     }
// }