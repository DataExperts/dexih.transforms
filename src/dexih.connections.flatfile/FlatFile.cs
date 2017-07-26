using dexih.functions;

namespace dexih.connections.flatfile
{
	public class FlatFile : Table
	{
		private string _fileRootPath;
		private string _fileIncomingPath;
		private string _fileProcessedPath;
		private string _fileRejectedPath;
		private string _fileMatchPattern;

		public bool UseCustomFilePaths { get; set; }

		public string FileRootPath {
			get 
			{
				return UseCustomFilePaths ? Name : _fileRootPath;
			}
			set 
			{
				_fileRootPath = value;
			}
		}

		public string FileIncomingPath
		{
			get
			{
				return UseCustomFilePaths ? "Incoming" : _fileIncomingPath;
			}
			set
			{
				_fileIncomingPath = value;
			}
		}

		public string FileProcessedPath
		{
			get
			{
				return UseCustomFilePaths ? "Processed" : _fileProcessedPath;
			}
			set
			{
				_fileProcessedPath = value;
			}
		}

		public string FileRejectedPath
		{
			get
			{
				return UseCustomFilePaths ? "Rejected" : _fileRejectedPath;
			}
			set
			{
				_fileRejectedPath = value;
			}
		}

		public string FileMatchPattern
		{
			get
			{
				return UseCustomFilePaths ? "*" : _fileMatchPattern;
			}
			set
			{
				_fileMatchPattern = value;
			}
		}

		public FileFormat FileFormat { get; set; }
		public string FileSample { get; set; }

		public FlatFile()
		{
		}
	}
}
