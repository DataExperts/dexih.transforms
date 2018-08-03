//using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms.Exceptions;

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
            ValidatePath(path);
            return path;
        }

        public override string GetFullPath(FlatFile file, EFlatFilePath path)
        {
            var fullPath = Path.Combine(FilePath(), file.FileRootPath ?? "", file.GetPath(path));
            ValidatePath(fullPath);
            return fullPath;
        }

        /// <summary>
        /// Applies checks on the path to ensure files can not be written to unauthorized locations
        /// </summary>
        /// <param name="path"></param>
        /// <exception cref="ForbiddenPathException"></exception>
        private void ValidatePath(string path)
        {
            if (string.IsNullOrEmpty(path))
            {
                throw new ForbiddenPathException($"An empty path is forbidden.  Path must start with a '/' or 'c:\\'.'.", path);
            }
            
            //check the path starts with a / or \ or c:\
            if (!(path.StartsWith("/") || path.StartsWith("\\") || (char.IsLetter(path[0]) && path[1] == ':')))
            {
                throw new ForbiddenPathException($"The path {path} is invalid as it does not start with a '/' or 'c:\\'.", path);
            }

            if (path.IndexOf("..", StringComparison.Ordinal) >= 0)
            {
                throw new ForbiddenPathException($"The path {path} is invalid as it contains a '..'.", path);
            }

            if (path.IndexOf("~", StringComparison.Ordinal) >= 0)
            {
                throw new ForbiddenPathException($"The path {path} is invalid as it contains a '~'.", path);
            }

            var normailzePath = NormalizePath(path);

            if (normailzePath.StartsWith(Directory.GetCurrentDirectory()))
            {
                throw new ForbiddenPathException($"The path {path} is invalid as it contains the remote agent binaries.", path);
            }

            if (AllowAllPaths)
            {
                return;
            }

            if (AllowedPaths == null || AllowedPaths.Length == 0)
            {
                throw new ForbiddenPathException($"The path cannot be validated as there are no allowed paths specified in the appsettings.json on the remote agent.", path);
            }

            foreach (var allowedPath in AllowedPaths)
            {
                if (normailzePath.StartsWith(NormalizePath(allowedPath)))
                {
                    return;
                }
            }
            
            throw new ForbiddenPathException($"The path {path} is forbidden as it is not within allowed paths.  To include this path, modify the AllowedFileDirectories section of the appsettings.json file on the remote agent.", path);
        }
        
        public static string NormalizePath(string path)
        {
            return Path.GetFullPath(new Uri(path).LocalPath)
                .TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .ToUpperInvariant();
        }

        public override Task<List<string>> GetFileShares()
        {
            try
            {
                ValidatePath(Server);

                var fileShares = new List<string>();
                var directories = Directory.GetDirectories(Server);
                foreach (var directoryName in directories)
                {
                    var directoryComponents = directoryName.Split(Path.DirectorySeparatorChar);
                    fileShares.Add(directoryComponents[directoryComponents.Length - 1]);
                }

                return Task.FromResult(fileShares);
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred getting file shares from {Server}.  {ex.Message}", ex);
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

        public override async Task<bool> MoveFile(FlatFile file, EFlatFilePath fromDirectory, EFlatFilePath toDirectory, string fileName)
        {
            try
            {
                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var fileNameExtension = Path.GetExtension(fileName);
                var version = 0;

                var newFileName = fileName;
                var fullToDirectory = GetFullPath(file, toDirectory);
                var fullFromDirectory = GetFullPath(file, fromDirectory);

                var createDirectoryResult = await CreateDirectory(file, toDirectory);
                if (!createDirectoryResult)
                {
                    return false;
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
                var files = new List<DexihFileProperties>();

                var fullDirectory = GetFullPath(file, path);
                var filenames = string.IsNullOrEmpty(searchPattern) ? Directory.GetFiles(fullDirectory) : Directory.GetFiles(fullDirectory, searchPattern);
                foreach (var fileName in filenames)
                {
                    var fileInfo = new FileInfo(fileName);
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

        public override Task<List<DexihFileProperties>> GetFileList(FlatFile file, EFlatFilePath path)
        {
            try
            {
                var files = new List<DexihFileProperties>();

                var fullDirectory = GetFullPath(file, path);
                foreach (var fileName in Directory.GetFiles(fullDirectory))
                {
                    var fileInfo = new FileInfo(fileName);
                    var contentType = ""; //MimeMapping.GetMimeMapping(FilePath + Path.DirectorySeparatorChar+ MainDirectory + Path.DirectorySeparatorChar+ SubDirectory + Path.DirectorySeparatorChar+ File); //TODO add MimeMapping
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


            var newFileName = fileName;
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

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken)
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
