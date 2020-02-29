using System.Runtime.Serialization;
using dexih.functions;
using Dexih.Utils.DataType;


namespace dexih.transforms.File
{
    [DataContract]
	public class WebService : Table
	{
        private string _restfulUri;

        /// <summary>
        /// The uri component of the web service call.  
        /// Parameters can be added using {param}
        /// For example: stream/{rows}
        /// </summary>
        [DataMember(Order = 0)]
        public string RestfulUri { 
            get => _restfulUri;
			set
            {
                if(!string.IsNullOrEmpty(value) && value[0] == '/')
                {
                    _restfulUri = value.Substring(1);
                }
                else
                {
                    _restfulUri = value;
                }
            }
        }

        [DataMember(Order = 1)]
        public string RowPath { get; set; }

        [DataMember(Order = 2)]
        public ETypeCode FormatType { get; set; } = ETypeCode.Json;

        // for text files
        [DataMember(Order = 3)]
        public FileConfiguration FileConfiguration { get; set; } = new FileConfiguration();
	
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
