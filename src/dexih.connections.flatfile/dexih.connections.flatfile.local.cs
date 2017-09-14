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
                List<string> fileShares = new List<string>();
            
                var directories = Directory.GetDirectories(serverName);
                foreach (string directoryName in directories)
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

                return Task.FromResult(true);
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
                string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                string fileNameExtension = Path.GetExtension(fileName);
                int version = 0;
                string newFileName;

                newFileName = fileName;
                var fullToDirectory = GetFullPath(file, toDirectory);
                var fullFromDirectory = GetFullPath(file, fromDirectory);

                var createDirectoryResult = await CreateDirectory(file, toDirectory);
                if (!createDirectoryResult)
                {
                    return createDirectoryResult;
                }

                // if there is already a file with the same name on the target directory, add a version number until a unique name is found.
                while (File.Exists(Path.Combine(fullToDirectory, newFileName)))
                {
                    version++;
                    newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
                }

                File.Move(Path.Combine(fullFromDirectory, fileName), Path.Combine(fullToDirectory, newFileName));
                return true;

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
                var fullDirectory = GetFullPath(file, path);
                File.Delete(Path.Combine(fullDirectory, fileName));
                return Task.FromResult(true);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred deleting file {file} at {path}.  {ex.Message}", ex);
            }
        }

        public override Task<DexihFiles> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern)
        {
            try
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
                return Task.FromResult(newFiles);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred getting files from {path} with pattern {searchPattern}.  {ex.Message}", ex);
            }
        }

        public override Task<List<DexihFileProperties>> GetFileList(FlatFile file, EFlatFilePath path)
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
                var fullDirectory = GetFullPath(file, path);
                Stream reader = File.OpenRead(Path.Combine(fullDirectory, fileName));
                return Task.FromResult(reader);
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
                var createDirectoryResult = await CreateDirectory(file, path);
                var fullDirectory = GetFullPath(file, path);
                Stream reader = File.OpenWrite(Path.Combine(fullDirectory, fileName));
                return reader;
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
					return true;
				}
				else 
				{
                    string filePath = FixFileName(file, path, fileName);
	                FileStream newFile = new FileStream(filePath, FileMode.Create, System.IO.FileAccess.Write);
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

        public override Task<bool> TestFileConnection()
        {
            try
            {
                bool exists = new DirectoryInfo(Server).Exists;
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

        public override Task<bool> TableExists(Table table, CancellationToken cancelToken)
        {
            try
            {
				FlatFile flatFile = (FlatFile)table;
                string fullPath = Path.Combine(FilePath(), flatFile.FileRootPath ?? "");

                bool exists = new DirectoryInfo(fullPath).Exists;
                return Task.FromResult(exists);
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Error occurred testing if a directory exists for flatfile {table.Name}.  {ex.Message}", ex);
            }
        }

    }
}
