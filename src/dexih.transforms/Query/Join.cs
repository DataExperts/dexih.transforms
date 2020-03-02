// using System.Text.Json.Serialization;
// using Dexih.Utils.CopyProperties;
// 
//
// namespace dexih.functions.Query
// {
//     public class Join
//     {
//         public Join()
//         {
//             
//         }
//
//         public Join(EJoinType joinType, string table, JoinFilters joinFilters)
//         {
//             JoinType = joinType;
//
//         }
//         
//         public Join(EJoinType joinType, string table, string alias, JoinFilters joinFilters)
//         {
//             JoinType = joinType;
//             JoinFilters = joinFilters;
//             Alias = alias;
//         }
//
//         [DataMember(Order = 0)]
//         public EJoinType JoinType { get; set; }
//         
//         [DataMember(Order = 1)]
//         public string JoinTable { get; set; }
//         
//         [DataMember(Order = 2)]
//         public JoinFilters JoinFilters { get; set; }
//         
//         [DataMember(Order = 3)]
//         public string Alias { get; set; }
//
//         private Table _table;
//         [JsonIgnore, CopyIgnore, MessagePack.IgnoreDataMember]
//         
//         public Table Table
//         {
//             get => _table??new Table(JoinTable);
//             set => _table = value;
//         }
//     }
// }