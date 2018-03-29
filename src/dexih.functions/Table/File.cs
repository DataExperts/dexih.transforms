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
			get => UseCustomFilePaths ? "incoming" : _fileIncomingPath;
			set => _fileIncomingPath = value;
		}

        public string FileOutgoingPath
        {
            get => AutoManageFiles ? (UseCustomFilePaths ? _fileOutgoingPath : "outgoing") : "";
            set => _fileOutgoingPath = value;
        }

        public string FileProcessedPath
		{
			get => UseCustomFilePaths ? "processed" : _fileProcessedPath;
			set => _fileProcessedPath = value;
		}

		public string FileRejectedPath
		{
			get => UseCustomFilePaths ? "rejected" : _fileRejectedPath;
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
	}
}
