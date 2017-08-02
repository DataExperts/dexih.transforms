using dexih.functions;

namespace dexih.connections.webservice
{
	public class RestFunction : Table
	{
        /// <summary>
        /// The uri component of the web service call.  
        /// Parameters can be added using {param}
        /// For example: stream/{rows}
        /// </summary>
		public string RestfulUri { get; set; }
		public string RowPath { get; set; }

		public RestFunction()
		{
		}

        public void AddInputParameter(string name, string defaultValue = null)
        {
            if(Columns.GetOrdinal(name) >= 0)
            {
                throw new TableDuplicateColumnNameException(this, name);
            }
            var column = new TableColumn(name)
            {
                IsInput = true,
                DefaultValue = defaultValue
            };
            Columns.Add(column);
        }
	}
}
