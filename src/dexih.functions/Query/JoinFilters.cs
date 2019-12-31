// using System.Collections.Generic;
// using System.Net.Sockets;
// using Dexih.Utils.DataType;
//
// namespace dexih.functions.Query
// {
//     public class JoinFilters: List<JoinFilter>
//     {
//         public JoinFilters() {}
//
//         public JoinFilters(IEnumerable<JoinFilter> filters)
//         {
//             if (filters == null) return;
//
//             base.AddRange(filters);
//         }
//
//         public JoinFilters(string columnName, object value)
//         {
//             Add(new JoinFilter(columnName, value));
//         }
//         
//         public JoinFilters(string columnName, ECompare compare, object value)
//         {
//             Add(new JoinFilter(columnName, compare, value));
//         }
//
//         public JoinFilters(params (string columnName, object value)[] filters)
//         {
//             foreach (var filter in filters)
//             {
//                 Add(new JoinFilter(filter.columnName, filter.value));
//             }
//         }
//         
//         public JoinFilters(params (string columnName, ECompare compare, object value)[] filters)
//         {
//             foreach (var filter in filters)
//             {
//                 Add(new JoinFilter(filter.columnName, filter.compare, filter.value));
//             }
//         }
//
//         public new void Add(JoinFilter filter)
//         {
//             if (!FilterExists(filter))
//             {
//                 base.Add(filter);
//             }
//         }
//
//         public bool FilterExists(JoinFilter checkFilter)
//         {
//             var exists = false;
//             foreach (var filter in this)
//             {
//                 if (filter.JoinColumn?.Name == checkFilter.JoinColumn?.Name && filter.Operator == checkFilter.Operator )
//                 {
//                     if(filter.InputColumn == null && checkFilter.InputColumn == null && checkFilter.InputValue == filter.InputValue) return true;
//                     if(filter.InputColumn != null && checkFilter.InputColumn != null && filter.InputColumn.Name == checkFilter.InputColumn.Name) return true;
//                 }
//                 if (filter.InputColumn?.Name == checkFilter.InputColumn?.Name && filter.Operator == checkFilter.Operator )
//                 {
//                     if(filter.JoinColumn == null && checkFilter.JoinColumn == null && checkFilter.JoinValue == filter.JoinValue) return true;
//                     if(filter.JoinColumn != null && checkFilter.JoinColumn != null && filter.JoinColumn.Name == checkFilter.JoinColumn.Name) return true;
//                 }
//             }
//
//             return exists;
//         }
//
//     }
// }