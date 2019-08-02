using dexih.functions.File;
using Dexih.Utils.DataType;

namespace dexih.functions
{
	public class FlatFile : Table
	{
		private string _fileRootPath;
		private string _fileIncomingPath = "incoming";
        private string _fileOutgoingPath = "outgoing";
		private string _fileProcessedPath = "processed";
		private string _fileRejectedPath = "rejected";
		private string _fileMatchPattern;

        public bool AutoManageFiles { get; set; }

        public bool UseCustomFilePaths { get; set; }

		public string FileRootPath {
			get => UseCustomFilePaths ? Name : _fileRootPath;
			set => _fileRootPath = value;
		}

		public string FileIncomingPath
		{
			get => AutoManageFiles ? ( UseCustomFilePaths ? _fileIncomingPath: "incoming") : "";
			set => _fileIncomingPath = value;
		}

        public string FileOutgoingPath
        {
            get => AutoManageFiles ? (UseCustomFilePaths ? _fileOutgoingPath : "outgoing") : "";
            set => _fileOutgoingPath = value;
        }

        public string FileProcessedPath
		{
			get => AutoManageFiles ? (UseCustomFilePaths ? _fileProcessedPath : "processed") : "";
            set => _fileProcessedPath = value;
		}

		public string FileRejectedPath
		{
			get => AutoManageFiles ? (UseCustomFilePaths ? _fileRejectedPath : "rejected") : "";
            set => _fileRejectedPath = value;
		}

		public string FileMatchPattern
		{
			get => UseCustomFilePaths ? "*" : _fileMatchPattern;
			set => _fileMatchPattern = value;
		}

		public DataType.ETypeCode FormatType { get; set; }
		public FileConfiguration FileConfiguration { get; set; } = new FileConfiguration();
		public string FileSample { get; set; }
		
		public string RowPath { get; set; }

        public string GetPath(EFlatFilePath path)
        {
            switch(path)
            {
                case EFlatFilePath.Incoming:
                    return FileIncomingPath;
                case EFlatFilePath.Outgoing:
                    return FileOutgoingPath;
                case EFlatFilePath.Processed:
                    return FileProcessedPath;
                case EFlatFilePath.Rejected:
                    return FileRejectedPath;
                case EFlatFilePath.None:
                    return "";
            }

            return "";
        }
        
        public FlatFile CopyFlatFile()
        {
	        var table = new FlatFile()
	        {
		        Description = Description,
		        Name = Name,
		        LogicalName = LogicalName,
		        AutoManageFiles = AutoManageFiles,
		        UseCustomFilePaths = UseCustomFilePaths,
		        FileIncomingPath = FileIncomingPath,
		        FileOutgoingPath = FileOutgoingPath,
		        FileProcessedPath = FileProcessedPath,
		        FileRejectedPath =  FileRejectedPath,
		        FileMatchPattern = FileMatchPattern,
		        FormatType = FormatType,
		        FileConfiguration = FileConfiguration,
		        RowPath = RowPath
	        };


	        foreach (var column in Columns)
	        {
		        var newCol = column.Copy();
		        table.Columns.Add(newCol);
	        }

	        return table;
        }
	}
}
