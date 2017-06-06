using System;
namespace dexih.functions
{
	public class File : Table
	{
		public bool UseCustomFilePaths { get; set; }
		public string FileRootPath { get; set; }
		public string FileIncomingPath { get; set; }
		public string FileProcessedPath { get; set; }
		public string FileRejectedPath { get; set; }
		public string FileMatchPattern { get; set; }

		public File()
		{
		}
	}
}
