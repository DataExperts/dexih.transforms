using dexih.functions;

namespace dexih.connections.flatfile
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

		public FileConfiguration FileConfiguration { get; set; } = new FileConfiguration();
		public string FileSample { get; set; }

		public FlatFile()
		{
		}

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
