using ProtoBuf;
using System;

namespace dexih.transforms
{

    [ProtoContract]
    [ProtoInclude(100, typeof(ConnectionReference))]
    public class ConnectionAttribute : Attribute
    {
        /// <summary>
        /// Category of the connection
        /// </summary>
        [ProtoMember(1)]
        public Connection.EConnectionCategory ConnectionCategory { get; set; }

        /// <summary>
        /// Connection Name
        /// </summary>
        [ProtoMember(2)]
        public string Name { get; set; }

        /// <summary>
        /// Description for the connection
        /// </summary>
        [ProtoMember(3)]
        public string Description { get; set; }

        /// <summary>
        /// Description for the database property (such as database, directory etc.)
        /// </summary>
        [ProtoMember(4)]
        public string DatabaseDescription { get; set; }

        /// <summary>
        /// Description for the server property (such as server name, web address etc.)
        /// </summary>
        [ProtoMember(5)]
        public string ServerDescription { get; set; }

        /// <summary>
        /// Allows for a connection string to use for credentials
        /// </summary>
        [ProtoMember(6)]
        public bool AllowsConnectionString { get; set; }

        /// <summary>
        /// Allows Sql Entry
        /// </summary>
        [ProtoMember(7)]
        public bool AllowsSql { get; set; }

        /// <summary>
        /// Uses files which can be managed (such as moving from incoming/processed directories.
        /// </summary>
        [ProtoMember(8)]
        public bool AllowsFlatFiles { get; set; }

        /// <summary>
        /// Can be used as a managed connection, supporting read/write and table create functions.
        /// </summary>
        [ProtoMember(9)]
        public bool AllowsManagedConnection { get; set; }

        /// <summary>
        /// Can be used a s source connection
        /// </summary>
        [ProtoMember(10)]
        public bool AllowsSourceConnection { get; set; }

        /// <summary>
        /// Can be used as a target connection
        /// </summary>
        [ProtoMember(11)]
        public bool AllowsTargetConnection { get; set; }

        /// <summary>
        /// Can use a username/password combination.
        /// </summary>
        [ProtoMember(12)]
        public bool AllowsUserPassword { get; set; }

        /// <summary>
        /// Can use windows authentication
        /// </summary>
        [ProtoMember(13)]
        public bool AllowsWindowsAuth { get; set; }

        /// <summary>
        /// Requires a database to be specified
        /// </summary>
        [ProtoMember(14)]
        public bool RequiresDatabase { get; set; }

        /// <summary>
        /// Requires access tothe local file system.
        /// </summary>
        [ProtoMember(15)]
        public bool RequiresLocalStorage { get; set; }
        
    }

}