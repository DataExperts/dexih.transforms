using dexih.functions;

namespace dexih.connections.flatfile
{
	public class FlatFile : Table
	{
        public enum EFlatFilePath
        {
            incoming, processed, rejected, none
        }

		private string _fileRootPath;
		private string _fileIncomingPath = "incoming";
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

		public string FileProcessedPath
		{
			get => AutoManageFiles ? (UseCustomFilePaths ? _fileIncomingPath : "processed") : "";
            set => _fileProcessedPath = value;
		}

		public string FileRejectedPath
		{
			get => AutoManageFiles ? (UseCustomFilePaths ? _fileIncomingPath : "rejected") : "";
            set => _fileRejectedPath = value;
		}

		public string FileMatchPattern
		{
			get => UseCustomFilePaths ? "*" : _fileMatchPattern;
			set => _fileMatchPattern = value;
		}

		public FileFormat FileFormat { get; set; } = new FileFormat();
		public string FileSample { get; set; }

		public FlatFile()
		{
		}

        public string GetPath(EFlatFilePath path)
        {
            switch(path)
            {
                case EFlatFilePath.incoming:
                    return FileIncomingPath;
                case EFlatFilePath.processed:
                    return FileProcessedPath;
                case EFlatFilePath.rejected:
                    return FileRejectedPath;
                case EFlatFilePath.none:
                    return "";
            }

            return "";
        }
	}
}
