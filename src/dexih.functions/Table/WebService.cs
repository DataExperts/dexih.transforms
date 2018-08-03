using dexih.functions.File;
using static Dexih.Utils.DataType.DataType;

namespace dexih.functions
{
	public class WebService : Table
	{
        private string _resetfulUri;

		/// <summary>
		/// The uri component of the web service call.  
		/// Parameters can be added using {param}
		/// For example: stream/{rows}
		/// </summary>
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
		public string RowPath { get; set; }

        public ETypeCode FormatType { get; set; } = ETypeCode.Json;
		
		// for text files
		public FileConfiguration FileConfiguration { get; set; } = new FileConfiguration();


		/// <summary>
		/// Maximum levels to recurse through structured data when importing columns.
		/// </summary>
		public int MaxImportLevels { get; set; } = 1;
	
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
