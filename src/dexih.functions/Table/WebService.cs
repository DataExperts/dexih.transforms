using dexih.functions.File;
using MessagePack;
using static Dexih.Utils.DataType.DataType;

namespace dexih.functions
{
    [MessagePackObject]
	public class WebService : Table
	{
        private string _resetfulUri;

        /// <summary>
        /// The uri component of the web service call.  
        /// Parameters can be added using {param}
        /// For example: stream/{rows}
        /// </summary>
        [Key(0)]
        public string RestfulUri { 
            get => _resetfulUri;
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

        [Key(1)]
        public string RowPath { get; set; }

        [Key(2)]
        public ETypeCode FormatType { get; set; } = ETypeCode.Json;

        // for text files
        [Key(3)]
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
