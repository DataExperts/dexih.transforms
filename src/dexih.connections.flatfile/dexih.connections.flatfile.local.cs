//using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;
using System.Data.Common;
using dexih.functions;

namespace dexih.connections.flatfile
{
    public class ConnectionFlatFileLocal : ConnectionFlatFile
    {
        public string FilePath()
        {
            return Server + "/" + DefaultDatabase;
        }

        public override async Task<ReturnValue<List<string>>> GetFileShares(string serverName, string userName, string password)
        {
            try
            {
                List<string> fileShares = new List<string>();
            
                var directories = await Task.Run(() => Directory.GetDirectories(serverName));
                foreach (string directoryName in directories)
                {
                    string[] directoryComponents = directoryName.Split('/');
                    fileShares.Add(directoryComponents[directoryComponents.Length - 1]);
                }

                return new ReturnValue<List<string>>(true, "", null, fileShares);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The following error occurred getting a list of directories: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue> CreateDirectory(string rootDirectory, string subDirectory)
        {
            try
            {
                return await Task.Run(() =>
                {
                    if (Directory.Exists(FilePath() + "/" + rootDirectory) == false)
                        Directory.CreateDirectory(FilePath() + "/" + rootDirectory);
                    Directory.CreateDirectory(FilePath() + "/" + rootDirectory + "/" + subDirectory);
                    return new ReturnValue(true);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred creating a directory: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> MoveFile(string rootDirectory, string fromDirectory, string toDirectory, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
                    string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                    string fileNameExtension = Path.GetExtension(fileName);
                    int version = 0;
                    string newFileName;

                    newFileName = fileName;
                    while (File.Exists(FilePath() + "/" + rootDirectory + "/" + toDirectory + "/" + newFileName))
                    {
                        version++;
                        newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
                    }
                    File.Move(FilePath() + "/" + rootDirectory + "/" + fromDirectory + "/" + fileName, FilePath() + "/" + rootDirectory + "/" + toDirectory + "/" + newFileName);
                    return new ReturnValue(true);
                });

            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred moving a file: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> DeleteFile(string rootDirectory, string subDirectory, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
                    File.Delete(FilePath() + "/" + rootDirectory + "/" + subDirectory + "/" + fileName);
                    return new ReturnValue(true);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred deleting a file: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<DexihFiles>> GetFileEnumerator(string mainDirectory, string subDirectory)
        {
            try
            {
                return await Task.Run(() =>
                {
                    List<DexihFileProperties> files = new List<DexihFileProperties>();

                    foreach (var file in Directory.GetFiles(FilePath() + "/" + mainDirectory + "/" + subDirectory))
                    {
                        FileInfo fileInfo = new FileInfo(file);
                        files.Add(new DexihFileProperties() { FileName = fileInfo.Name, LastModified = fileInfo.LastWriteTime, Length = fileInfo.Length });
                    }

                    DexihFiles newFiles = new DexihFiles(files.ToArray());
                    return new ReturnValue<DexihFiles>(true, "", null, newFiles);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue<DexihFiles>(false, "The following error occurred getting a list of files: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<List<DexihFileProperties>>> GetFileList(string mainDirectory, string subDirectory)
        {
            try
            {
                return await Task.Run(() =>
                {
                    List<DexihFileProperties> files = new List<DexihFileProperties>();

                    foreach (var file in Directory.GetFiles(FilePath() + "/" + mainDirectory + "/" + subDirectory))
                    {
                        FileInfo fileInfo = new FileInfo(file);
                        string contentType = ""; //MimeMapping.GetMimeMapping(FilePath + "/" + MainDirectory + "/" + SubDirectory + "/" + File); //TODO add MimeMapping
                        files.Add(new DexihFileProperties() { FileName = fileInfo.Name, LastModified = fileInfo.LastWriteTime, Length = fileInfo.Length, ContentType = contentType });
                    }

                    return new ReturnValue<List<DexihFileProperties>>(true, "", null, files);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<DexihFileProperties>>(false, "The following error occurred getting a list of files: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Stream>> GetReadFileStream(Table table, string subDirectory, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
					FlatFile flatFile = (FlatFile)table;
					Stream reader = File.OpenRead(FilePath() + "/" + flatFile.FileRootPath + "/" + subDirectory + "/" + "/" + fileName);
                    return new ReturnValue<Stream>(true, "", null, reader);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue<Stream>(false, "The following error occurred opening a file read stream: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Stream>> GetWriteFileStream(Table table, string subDirectory, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
					FlatFile flatFile = (FlatFile)table;
                    Stream reader = File.OpenWrite(FilePath() + "/" + flatFile.FileRootPath + "/" + subDirectory + "/" + "/" + fileName);
                    return new ReturnValue<Stream>(true, "", null, reader);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue<Stream>(false, "The following error occurred opening a file write stream: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue> SaveFileStream(Table table, string fileName, Stream stream)
        {
            try
            {
                string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                string fileNameExtension = Path.GetExtension(fileName);

				if(fileNameExtension == ".zip") 
				{
					using (ZipArchive archive = new ZipArchive(stream, ZipArchiveMode.Read))
	                {
                        foreach(var entry in archive.Entries)
                        {
                            string filePath = FixFileName(table, entry.Name);
                            entry.ExtractToFile(filePath);
                        }

	                }
					return new ReturnValue(true);
				}
				else 
				{
                    string filePath = FixFileName(table, fileName);
	                FileStream newFile = new FileStream(filePath, FileMode.Create, System.IO.FileAccess.Write);
	                //stream.Seek(0, SeekOrigin.Begin);
	                await stream.CopyToAsync(newFile);
	                await stream.FlushAsync();
	                newFile.Dispose();

	                return new ReturnValue(true);
				}
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred creating saving file stream: " + ex.Message, ex);
            }
        }

        private string FixFileName(Table table, string fileName)
        {
			FlatFile flatFile = (FlatFile)table;

			string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
			string fileNameExtension = Path.GetExtension(fileName);

			int version = 0;

			string newFileName = fileName;
			while (File.Exists(FilePath() + "/" + flatFile.FileRootPath + "/" + flatFile.FileIncomingPath + "/" + newFileName))
			{
				version++;
				newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
			}

			var filePath = FilePath() + "/" + flatFile.FileRootPath + "/" + flatFile.FileIncomingPath + "/" + newFileName;

			return filePath;
		}

        public override async Task<ReturnValue> TestFileConnection()
        {
            try
            {
                bool exists = await Task.Run(() => new DirectoryInfo(Server).Exists);
                if (exists)
                    State = EConnectionState.Open;
                else
                    State = EConnectionState.Broken;

                return new ReturnValue(exists);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred creating testing the file connection: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<bool>> TableExists(Table table)
        {
            try
            {
				FlatFile flatFile = (FlatFile)table;

				bool exists = await Task.Run(() => new DirectoryInfo(FilePath() + "/" + flatFile.FileRootPath).Exists);
                return new ReturnValue<bool>(true, exists);
            }
            catch(Exception ex)
            {
                return new ReturnValue<bool>(false, "The following error occurred testing if a directory exists: " + ex.Message, ex);
            }
        }

    }
}
