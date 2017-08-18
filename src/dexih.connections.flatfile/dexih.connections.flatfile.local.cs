//using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using static dexih.connections.flatfile.FlatFile;

namespace dexih.connections.flatfile
{
    public class ConnectionFlatFileLocal : ConnectionFlatFile
    {
        public string FilePath()
        {
            return Path.Combine(Server ,DefaultDatabase ?? "");
        }

        private string GetFullPath(FlatFile file, EFlatFilePath path)
        {
            var fullPath = Path.Combine(FilePath(), file.FileRootPath ?? "", file.GetPath(path));
            return fullPath;
        }

        public override async Task<ReturnValue<List<string>>> GetFileShares(string serverName, string userName, string password)
        {
            try
            {
                List<string> fileShares = new List<string>();
            
                var directories = await Task.Run(() => Directory.GetDirectories(serverName));
                foreach (string directoryName in directories)
                {
                    string[] directoryComponents = directoryName.Split(Path.DirectorySeparatorChar);
                    fileShares.Add(directoryComponents[directoryComponents.Length - 1]);
                }

                return new ReturnValue<List<string>>(true, "", null, fileShares);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The following error occurred getting a list of directories: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue> CreateDirectory(FlatFile file, EFlatFilePath path)
        {
            try
            {
                return await Task.Run(() =>
                {
                    var directory = FilePath();
                    if (!Directory.Exists(directory))
                    {
                        Directory.CreateDirectory(directory);
                    }

                    if(file != null && string.IsNullOrEmpty(file.FileRootPath))
                    {
                        directory = Path.Combine(FilePath(), file.FileRootPath);
                        if (!Directory.Exists(directory))
                        {
                            Directory.CreateDirectory(directory);
                        }
                    }

                    if(file != null &&  path != EFlatFilePath.none)
                    {
                        directory = Path.Combine(directory, file.GetPath(path));
                        if (!Directory.Exists(directory))
                        {
                            Directory.CreateDirectory(directory);
                        }
                    }

                    return new ReturnValue(true);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred creating a directory: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> MoveFile(FlatFile file, EFlatFilePath fromDirectory, EFlatFilePath toDirectory, string fileName)
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
                    var fullToDirectory = GetFullPath(file, toDirectory);
                    var fullFromDirectory = GetFullPath(file, fromDirectory);

                    if(!Directory.Exists(fullToDirectory))
                    {
                        Directory.CreateDirectory(fullToDirectory);
                    }

                    // if there is already a file with the same name on the target directory, add a version number until a unique name is found.
                    while (File.Exists(Path.Combine(fullToDirectory, newFileName)))
                    {
                        version++;
                        newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
                    }

                    File.Move(Path.Combine(fullFromDirectory, fileName), Path.Combine(fullToDirectory, newFileName));
                    return new ReturnValue(true);
                });

            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred moving a file: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> DeleteFile(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
                    var fullDirectory = GetFullPath(file, path);
                    File.Delete(Path.Combine(fullDirectory, fileName));
                    return new ReturnValue(true);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred deleting a file: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<DexihFiles>> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern)
        {
            try
            {
                return await Task.Run(() =>
                {
                    List<DexihFileProperties> files = new List<DexihFileProperties>();

                    var fullDirectory = GetFullPath(file, path);
                    var filenames = string.IsNullOrEmpty(searchPattern) ? Directory.GetFiles(fullDirectory) : Directory.GetFiles(fullDirectory, searchPattern);
                    foreach (var fileName in filenames)
                    {
                        FileInfo fileInfo = new FileInfo(fileName);
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

        public override async Task<ReturnValue<List<DexihFileProperties>>> GetFileList(FlatFile file, EFlatFilePath path)
        {
            return await Task.Run(() =>
            {
                try
                {
                        List<DexihFileProperties> files = new List<DexihFileProperties>();

                        var fullDirectory = GetFullPath(file, path);
                        foreach (var fileName in Directory.GetFiles(fullDirectory))
                        {
                            FileInfo fileInfo = new FileInfo(fileName);
                            string contentType = ""; //MimeMapping.GetMimeMapping(FilePath + Path.DirectorySeparatorChar+ MainDirectory + Path.DirectorySeparatorChar+ SubDirectory + Path.DirectorySeparatorChar+ File); //TODO add MimeMapping
                            files.Add(new DexihFileProperties() { FileName = fileInfo.Name, LastModified = fileInfo.LastWriteTime, Length = fileInfo.Length, ContentType = contentType });
                        }

                        return new ReturnValue<List<DexihFileProperties>>(true, "", null, files);
                }
                catch (Exception ex)
                {
                    return new ReturnValue<List<DexihFileProperties>>(false, "The following error occurred getting a list of files: " + ex.Message, ex, null);
                }
            });
        }

        public override async Task<ReturnValue<Stream>> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
                    var fullDirectory = GetFullPath(file, path);
                    Stream reader = File.OpenRead(Path.Combine(fullDirectory, fileName));
                    return new ReturnValue<Stream>(true, "", null, reader);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue<Stream>(false, "The following error occurred opening a file read stream: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Stream>> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
                    var fullDirectory = GetFullPath(file, path);
                    Stream reader = File.OpenWrite(Path.Combine(fullDirectory, fileName));
                    return new ReturnValue<Stream>(true, "", null, reader);
                });
            }
            catch (Exception ex)
            {
                return new ReturnValue<Stream>(false, "The following error occurred opening a file write stream: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream stream)
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
                            string filePath = FixFileName(file, path, entry.Name);
                            entry.ExtractToFile(filePath);
                        }

	                }
					return new ReturnValue(true);
				}
				else 
				{
                    string filePath = FixFileName(file, path, fileName);
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

        private string FixFileName(FlatFile file, EFlatFilePath path, string fileName)
        {
			string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
			string fileNameExtension = Path.GetExtension(fileName);

			int version = 0;

            string fullPath = GetFullPath(file, path);


            string newFileName = fileName;
			while (File.Exists(Path.Combine(fullPath, newFileName)))
			{
				version++;
				newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
			}

			var filePath = Path.Combine(fullPath, newFileName);

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

        public override async Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
            try
            {
				FlatFile flatFile = (FlatFile)table;
                string fullPath = Path.Combine(FilePath(), flatFile.FileRootPath ?? "");

                bool exists = await Task.Run(() => new DirectoryInfo(fullPath).Exists, cancelToken);
                return new ReturnValue<bool>(true, exists);
            }
            catch(Exception ex)
            {
                return new ReturnValue<bool>(false, "The following error occurred testing if a directory exists: " + ex.Message, ex);
            }
        }

    }
}
