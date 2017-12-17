using dexih.functions;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.webservice
{
	public class RestFunction : Table
	{
        private string _resetfulUri;

		/// <summary>
		/// The uri component of the web service call.  
		/// Parameters can be added using {param}
		/// For example: stream/{rows}
		/// </summary>
		public string RestfulUri { 
            get
            {
                return _resetfulUri;
            }
            set
            {
                if(!string.IsNullOrEmpty(value) && value[0] == '/')
                {
                    _resetfulUri = value.Substring(1);
                }
                else
                {
                    _resetfulUri = value;
                }
            }
        }
		public string RowPath { get; set; }

        public ETypeCode FormatType { get; set; }

		public void AddInputParameter(string name, string defaultValue = null)
		{
			if (Columns.GetOrdinal(name) >= 0)
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
