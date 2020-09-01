using System;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.functions.Query
{
    public class Join
    {
        public Join()
        {
            JoinId = Guid.NewGuid().ToString();
        }

        public Join(EJoinType joinType, Table table, Filters joinFilters)
        {
            JoinType = joinType;
            JoinTable = table;
            JoinFilters = joinFilters;
            JoinId = Guid.NewGuid().ToString();
        }
        
        public Join(EJoinType joinType, Table table, string alias, Filters joinFilters)
        {
            JoinType = joinType;
            JoinFilters = joinFilters;
            JoinTable = table;
            Alias = alias;
            JoinId = Guid.NewGuid().ToString();
        }

        [DataMember(Order = 0)]
        public EJoinType JoinType { get; set; }
        
        [DataMember(Order = 1)]
        public Table JoinTable { get; set; }
        
        [DataMember(Order = 2)]
        public Filters JoinFilters { get; set; }
        
        [DataMember(Order = 3)]
        public string Alias { get; set; }
        
        [DataMember(Order = 4)]
        public int ConnectionId { get; set; }

        /// <summary>
        /// Unique id to remember the join.
        /// </summary>
        public string JoinId { get; set; }
    }
}