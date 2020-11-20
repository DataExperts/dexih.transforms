
using System;
using System.Runtime.Serialization;

namespace dexih.transforms
{

    [DataContract]
    // [Union(0, typeof(ConnectionReference))]
    public class ConnectionAttribute : Attribute
    {
        // [IgnoreDataMember]
        // public override object TypeId { get; }

        /// <summary>
        /// Category of the connection
        /// </summary>
        [DataMember(Order = 0)]
        public Connection.EConnectionCategory ConnectionCategory { get; set; }

        /// <summary>
        /// Connection Name
        /// </summary>
        [DataMember(Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Description for the connection
        /// </summary>
        [DataMember(Order = 2)]
        public string Description { get; set; }

        /// <summary>
        /// Description for the database property (such as database, directory etc.)
        /// </summary>
        [DataMember(Order = 3)]
        public string DatabaseDescription { get; set; }

        /// <summary>
        /// Description for the server property (such as server name, web address etc.)
        /// </summary>
        [DataMember(Order = 4)]
        public string ServerDescription { get; set; }

        /// <summary>
        /// Note for the server property (such as how to format the server name.)
        /// </summary>
        [DataMember(Order = 5)]
        public string ServerHelp { get; set; }

        /// <summary>
        /// Allows for a connection string to use for credentials
        /// </summary>
        [DataMember(Order = 6)]
        public bool AllowsConnectionString { get; set; }

        /// <summary>
        /// Allows Sql Entry
        /// </summary>
        [DataMember(Order = 7)]
        public bool AllowsSql { get; set; }

        /// <summary>
        /// Uses files which can be managed (such as moving from incoming/processed directories.
        /// </summary>
        [DataMember(Order = 8)]
        public bool AllowsFlatFiles { get; set; }

        /// <summary>
        /// Can be used as a managed connection, supporting read/write and table create functions.
        /// </summary>
        [DataMember(Order = 9)]
        public bool AllowsManagedConnection { get; set; }

        /// <summary>
        /// Can be used a s source connection
        /// </summary>
        [DataMember(Order = 10)]
        public bool AllowsSourceConnection { get; set; }

        /// <summary>
        /// Can be used as a target connection
        /// </summary>
        [DataMember(Order = 11)]
        public bool AllowsTargetConnection { get; set; }

        /// <summary>
        /// Can use a username/password combination.
        /// </summary>
        [DataMember(Order = 12)]
        public bool AllowsUserPassword { get; set; }

        /// <summary>
        /// Can use a secure token.
        /// </summary>
        [DataMember(Order = 13)]
        public bool AllowsToken { get; set; }
        
        /// <summary>
        /// Can use windows authentication
        /// </summary>
        [DataMember(Order = 14)]
        public bool AllowsWindowsAuth { get; set; }

        /// <summary>
        /// Requires a database to be specified
        /// </summary>
        [DataMember(Order = 15)]
        public bool RequiresDatabase { get; set; }

        /// <summary>
        /// Requires access to the local file system.
        /// </summary>
        [DataMember(Order = 16)]
        public bool RequiresLocalStorage { get; set; }
        
        [DataMember(Order=17)]
        public string DefaultSchema { get; set; }
    }

}