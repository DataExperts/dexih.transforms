using System;

namespace dexih.transforms
{

    
    public class ConnectionAttribute : Attribute
    {
        /// <summary>
        /// Category of the connection
        /// </summary>
        public Connection.EConnectionCategory ConnectionCategory { get; set; }
        
        /// <summary>
        /// Connection Name
        /// </summary>
        public string Name { get; set; }
        
        /// <summary>
        /// Description for the connection
        /// </summary>
        public string Description { get; set; }
        
        /// <summary>
        /// Description for the database property (such as database, directory etc.)
        /// </summary>
        public string DatabaseDescription { get; set; }
        
        /// <summary>
        /// Description for the server property (such as server name, web address etc.)
        /// </summary>
        public string ServerDescription { get; set; }
        
        /// <summary>
        /// Allows for a connection string to use for credentials
        /// </summary>
        public bool AllowsConnectionString { get; set; }
        
        /// <summary>
        /// Allows Sql Entry
        /// </summary>
        public bool AllowsSql { get; set; }
        
        /// <summary>
        /// Uses files which can be managed (such as moving from incoming/processed directories.
        /// </summary>
        public bool AllowsFlatFiles { get; set; }
        
        /// <summary>
        /// Can be used as a managed connection, supporting read/write and table create functions.
        /// </summary>
        public bool AllowsManagedConnection { get; set; }
        
        /// <summary>
        /// Can be used a s source connection
        /// </summary>
        public bool AllowsSourceConnection { get; set; }
        
        /// <summary>
        /// Can be used as a target connection
        /// </summary>
        public bool AllowsTargetConnection { get; set; }
        
        /// <summary>
        /// Can use a username/password combination.
        /// </summary>
        public bool AllowsUserPassword { get; set; }
        
        /// <summary>
        /// Can use windows authentication
        /// </summary>
        public bool AllowsWindowsAuth { get; set; }
        
        /// <summary>
        /// Requires a database to be specified
        /// </summary>
        public bool RequiresDatabase { get; set; }
        
        /// <summary>
        /// Requires access tothe local file system.
        /// </summary>
        public bool RequiresLocalStorage { get; set; }
        
    }

}