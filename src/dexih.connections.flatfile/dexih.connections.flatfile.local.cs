//using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;
using dexih.transforms.File;

namespace dexih.connections.flatfile
{
    [Connection(
        ConnectionCategory = EConnectionCategory.File,
        Name = "Local Flat File", 
        Description = "Local Flat File storage",
        DatabaseDescription = "Sub Directory",
        ServerDescription = "Full Directory",
        AllowsConnectionString = false,
        AllowsSql = false,
        AllowsFlatFiles = true,
        AllowsManagedConnection = false,
        AllowsSourceConnection = true,
        AllowsTargetConnection = true,
        AllowsUserPassword = false,
        AllowsWindowsAuth = false,
        RequiresDatabase = false,
        RequiresLocalStorage = true
    )]
    public class ConnectionFlatFileLocal : ConnectionFlatFile
    {
        public string FilePath()
        {
            var path = Path.Combine(Server ?? "" ,DefaultDatabase ?? "");
            FilePermissions?.ValidatePath(path);
            return path;
        }

        public override string GetFullPath(FlatFile file, EFlatFilePath path)
        {
            var fullPath = Path.Combine(FilePath(), file.FileRootPath ?? "", file.GetPath(path));
            FilePermissions?.ValidatePath(fullPath);
            return fullPath;
        }

        public override Task<List<string>> GetFileShares(CancellationToken cancellationToken)
        {
            try
            {
                FilePermissions?.ValidatePath(Server);

                var fileShares = new List<string>();
                var directories = Directory.GetDirectories(Server);
                foreach (var directoryName in directories)
                {
                    var directoryComponents = directoryName.Split(Path.DirectorySeparatorChar);
                    fileShares.Add(directoryComponents[^1]);
                }

                return Task.FromResult(fileShares);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred getting file shares from {Server}.  {ex.Message}", ex);
            }
        }

        public override Task<bool> CreateDirectory(FlatFile file, EFlatFilePath path, CancellationToken cancellationToken)
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

                if(file != null &&  path != EFlatFilePath.None)
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

        public override async Task<bool> MoveFile(FlatFile file, EFlatFilePath fromDirectory, EFlatFilePath toDirectory, string fileName, CancellationToken cancellationToken)
        {
            try
            {
                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var fileNameExtension = Path.GetExtension(fileName);
                var version = 0;

                var newFileName = fileName;
                var fullToDirectory = GetFullPath(file, toDirectory);
                var fullFromDirectory = GetFullPath(file, fromDirectory);

                var createDirectoryResult = await CreateDirectory(file, toDirectory, cancellationToken);
                if (!createDirectoryResult)
                {
                    return false;
                }

                // if there is already a file with the same name on the target directory, add a version number until a unique name is found.
                while (File.Exists(Path.Combine(fullToDirectory, newFileName)))
                {
                    version++;
                    newFileName = fileNameWithoutExtension + "_" + version + fileNameExtension;
                }

                File.Move(Path.Combine(fullFromDirectory, fileName), Path.Combine(fullToDirectory, newFileName));
                return true;

            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred moving file {file.Name} from {fromDirectory} to {toDirectory}.  {ex.Message}", ex);
            }
        }

        public override Task<bool> DeleteFile(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
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

        public override async IAsyncEnumerable<FileProperties> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
                var fullDirectory = GetFullPath(file, path);
                var filenames = await Task.Run(() => string.IsNullOrEmpty(searchPattern) ? Directory.GetFiles(fullDirectory) : Directory.GetFiles(fullDirectory, searchPattern), cancellationToken);
                foreach (var fileName in filenames)
                {
                    var properties = new FileProperties()
                    {
                        FileName = fileName,
                    };
                    
                    var hasLoaded = false;
                    properties.LoadAttributes = () => 
                    {
                        if (!hasLoaded)
                        {
                            hasLoaded = true;
                            var fileInfo = new FileInfo(fileName);
                            properties.LastModified = fileInfo.LastWriteTime;
                            properties.Length = fileInfo.Length;
                        }
                        
                        return Task.CompletedTask;
                    };

                    yield return properties;
                }
        }

        public override Task<Stream> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
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

        public override async Task<Stream> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
            try
            {
                await CreateDirectory(file, path, cancellationToken);
                var fullDirectory = GetFullPath(file, path);
                Stream reader = File.OpenWrite(Path.Combine(fullDirectory, fileName));
                return reader;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred writing file {fileName} at {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<bool> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream stream, CancellationToken cancellationToken)
        {
            try
            {
                await CreateDirectory(file, path, cancellationToken);

//                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
//                var fileNameExtension = Path.GetExtension(fileName);
//
//				if(fileNameExtension == ".zip") 
//				{
//					using (var archive = new ZipArchive(stream, ZipArchiveMode.Read))
//	                {
//                        foreach(var entry in archive.Entries)
//                        {
//                            if(string.IsNullOrEmpty(entry.Name) || entry.FullName.StartsWith("__MACOSX") || entry.Length == 0) continue;
//                           
//                            var filePath = FixFileName(file, path, entry.Name);
//                            entry.ExtractToFile(filePath);
//                        }
//
//	                }
//					return true;
//				} else if (fileNameExtension == ".gz")
//                {
//                    using (var decompressionStream = new GZipStream(stream, CompressionMode.Decompress))
//                    {
//                        var newFileName = fileName.Substring(0, fileName.Length - 3);
//                        var filePath = FixFileName(file, path, newFileName);
//                        var newFile = new FileStream(filePath, FileMode.Create, FileAccess.Write);
//                        //stream.Seek(0, SeekOrigin.Begin);
//                        await decompressionStream.CopyToAsync(newFile);
//                        await decompressionStream.FlushAsync();
//                        newFile.Dispose();
//                    }
//                    
//                    return true;
//                }
//				else 
//				{
                    var filePath = FixFileName(file, path, fileName, cancellationToken);
	                var newFile = new FileStream(filePath, FileMode.Create, FileAccess.Write);
	                //stream.Seek(0, SeekOrigin.Begin);
	                await stream.CopyToAsync(newFile, cancellationToken);
	                stream.Close();
	                await newFile.DisposeAsync();

	                return true;
//				}
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred saving file stream of file {fileName} at {path}.  {ex.Message}", ex);
            }
        }

        private string FixFileName(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
			var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
			var fileNameExtension = Path.GetExtension(fileName);

			var version = 0;

            var fullPath = GetFullPath(file, path);


            var newFileName = fileName;
			while (File.Exists(Path.Combine(fullPath, newFileName)))
			{
				version++;
				newFileName = fileNameWithoutExtension + "_" + version + fileNameExtension;
			}

			var filePath = Path.Combine(fullPath, newFileName);

			return filePath;
		}

        public override Task<bool> TestFileConnection(CancellationToken cancellationToken)
        {
            try
            {
                var path = FilePath();
                var exists = new DirectoryInfo(path).Exists;
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

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
				var flatFile = (FlatFile)table;
                var fullPath = Path.Combine(FilePath(), flatFile.FileRootPath ?? "");

                var exists = new DirectoryInfo(fullPath).Exists;
                return Task.FromResult(exists);
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Error occurred testing if a directory exists for flatfile {table.Name}.  {ex.Message}", ex);
            }
        }
    }
}
