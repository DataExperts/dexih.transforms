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
using dexih.transforms.Exceptions;

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

        public override Task<List<string>> GetFileShares(string serverName, string userName, string password)
        {
            try
            {
                var fileShares = new List<string>();
            
                var directories = Directory.GetDirectories(serverName);
                foreach (var directoryName in directories)
                {
                    string[] directoryComponents = directoryName.Split(Path.DirectorySeparatorChar);
                    fileShares.Add(directoryComponents[directoryComponents.Length - 1]);
                }

                return Task.FromResult(fileShares);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred getting file shares from {serverName}.  {ex.Message}", ex);
            }
        }

        public override Task<bool> CreateDirectory(FlatFile file, EFlatFilePath path)
        {
            try
            {
                var directory = FilePath();
                if (!Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                if(file != null && !string.IsNullOrEmpty(file.FileRootPath))
                {
                    var fullBaseDirectory = Path.Combine(FilePath(), rootDirectory ?? "");
                    if (!Directory.Exists(fullBaseDirectory))
                    {
                        Directory.CreateDirectory(fullBaseDirectory);
                    }
                    var fullSubDirectory = Path.Combine(fullBaseDirectory, subDirectory ?? "");
                    if(!Directory.Exists(fullSubDirectory))
                    {
                        Directory.CreateDirectory(fullSubDirectory);
                    }
                    return new ReturnValue(true);
                });
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred creating directory {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<bool> MoveFile(FlatFile file, EFlatFilePath fromDirectory, EFlatFilePath toDirectory, string fileName)
        {
            try
            {
                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var fileNameExtension = Path.GetExtension(fileName);
                var version = 0;
                string newFileName;

                newFileName = fileName;
                var fullToDirectory = GetFullPath(file, toDirectory);
                var fullFromDirectory = GetFullPath(file, fromDirectory);

                var createDirectoryResult = await CreateDirectory(file, toDirectory);
                if (!createDirectoryResult)
                {
                    return createDirectoryResult;
                }

                    newFileName = fileName;
                    var fullToDirectory = Path.Combine(FilePath(), rootDirectory ?? "", toDirectory ?? "");
                    var fullFromDirectory = Path.Combine(FilePath(), rootDirectory ?? "", fromDirectory ?? "");

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
                throw new ConnectionException($"Error occurred moving file {file.Name} from {fromDirectory} to {toDirectory}.  {ex.Message}", ex);
            }
        }

        public override Task<bool> DeleteFile(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
                    var fullDirectory = Path.Combine(FilePath(), rootDirectory ?? "", subDirectory ?? "");
                    File.Delete(Path.Combine(fullDirectory, fileName));
                    return new ReturnValue(true);
                });
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred deleting file {file} at {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<ReturnValue<DexihFiles>> GetFileEnumerator(string rootDirectory, string subDirectory, string searchPattern)
        {
            try
            {
                var files = new List<DexihFileProperties>();

                    var fullDirectory = Path.Combine(FilePath(), rootDirectory??"", subDirectory??"");
                    var filenames = string.IsNullOrEmpty(searchPattern) ? Directory.GetFiles(fullDirectory) : Directory.GetFiles(fullDirectory, searchPattern);
                    foreach (var file in filenames)
                    {
                        FileInfo fileInfo = new FileInfo(file);
                        files.Add(new DexihFileProperties() { FileName = fileInfo.Name, LastModified = fileInfo.LastWriteTime, Length = fileInfo.Length });
                    }

                var newFiles = new DexihFiles(files.ToArray());
                return Task.FromResult(newFiles);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred getting files from {path} with pattern {searchPattern}.  {ex.Message}", ex);
            }
        }

        public override async Task<ReturnValue<List<DexihFileProperties>>> GetFileList(string rootDirectory, string subDirectory)
        {
            try
            {
                var files = new List<DexihFileProperties>();

                    var fullDirectory = Path.Combine(FilePath(), rootDirectory??"", subDirectory ?? "");
                    foreach (var file in Directory.GetFiles(fullDirectory))
                    {
                        FileInfo fileInfo = new FileInfo(file);
                        string contentType = ""; //MimeMapping.GetMimeMapping(FilePath + Path.DirectorySeparatorChar+ MainDirectory + Path.DirectorySeparatorChar+ SubDirectory + Path.DirectorySeparatorChar+ File); //TODO add MimeMapping
                        files.Add(new DexihFileProperties() { FileName = fileInfo.Name, LastModified = fileInfo.LastWriteTime, Length = fileInfo.Length, ContentType = contentType });
                    }

                return Task.FromResult(files);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred getting filelist {path}.  {ex.Message}", ex);
            }
        }

        public override Task<Stream> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
					FlatFile flatFile = (FlatFile)table;
                    var fullDirectory = Path.Combine(FilePath(), flatFile.FileRootPath ?? "", subDirectory ?? "");
                    Stream reader = File.OpenRead(Path.Combine(fullDirectory, fileName));
                    return new ReturnValue<Stream>(true, "", null, reader);
                });
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred reading file {fileName} at {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<Stream> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                return await Task.Run(() =>
                {
					FlatFile flatFile = (FlatFile)table;
                    var fullDirectory = Path.Combine(FilePath(), flatFile.FileRootPath ?? "", subDirectory ?? "");
                    Stream reader = File.OpenWrite(Path.Combine(fullDirectory, fileName));
                    return new ReturnValue<Stream>(true, "", null, reader);
                });
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred writing file {fileName} at {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<bool> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream stream)
        {
            try
            {
                var createDirectoryResult = await CreateDirectory(file, path);

                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var fileNameExtension = Path.GetExtension(fileName);

				if(fileNameExtension == ".zip") 
				{
					using (var archive = new ZipArchive(stream, ZipArchiveMode.Read))
	                {
                        foreach(var entry in archive.Entries)
                        {
                            var filePath = FixFileName(file, path, entry.Name);
                            entry.ExtractToFile(filePath);
                        }

	                }
					return true;
				}
				else 
				{
                    var filePath = FixFileName(file, path, fileName);
	                var newFile = new FileStream(filePath, FileMode.Create, System.IO.FileAccess.Write);
	                //stream.Seek(0, SeekOrigin.Begin);
	                await stream.CopyToAsync(newFile);
	                await stream.FlushAsync();
	                newFile.Dispose();

	                return true;
				}
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred saving file stream of file {fileName} at {path}.  {ex.Message}", ex);
            }
        }

        private string FixFileName(FlatFile file, EFlatFilePath path, string fileName)
        {
			var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
			var fileNameExtension = Path.GetExtension(fileName);

			var version = 0;

            var fullPath = GetFullPath(file, path);


            string fullPath = Path.Combine(FilePath(), flatFile.FileRootPath ?? "", flatFile.FileIncomingPath ?? "");


            string newFileName = fileName;
			while (File.Exists(Path.Combine(fullPath, newFileName)))
			{
				version++;
				newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
			}

			var filePath = Path.Combine(fullPath, newFileName);

			return filePath;
		}

        public override Task<bool> TestFileConnection()
        {
            try
            {
                var exists = new DirectoryInfo(Server).Exists;
                if (exists)
                    State = EConnectionState.Open;
                else
                    State = EConnectionState.Broken;

                return Task.FromResult(exists);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred testing if directory exists {Server}.  {ex.Message}", ex);
            }
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken)
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
                throw new ConnectionException($"Error occurred testing if a directory exists for flatfile {table.Name}.  {ex.Message}", ex);
            }
        }

    }
}
