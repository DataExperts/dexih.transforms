//using dexih.functions;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.connections.flatfile;
using dexih.functions;
using dexih.transforms.Exceptions;
using Renci.SshNet;

namespace dexih.connections.sftp
{
    public class ConnectionFlatFileSftp : ConnectionFlatFile
    {
        public override string GetFullPath(FlatFile file, EFlatFilePath path)
        {
            var fullPath = DefaultDatabase;
            if (!string.IsNullOrEmpty(file.FileRootPath))
            {
                fullPath += "/" + file.FileRootPath;
            }

            var subPath = file.GetPath(path);
            if (!string.IsNullOrEmpty(subPath))
            {
                fullPath += "/" + subPath;
            }
                
            return fullPath;
        }

        private string CombinePath(string path, string filename)
        {
            return path + "/" + filename;
        }

        private SftpClient GetSftpClient()
        {
            string[] paths;
            
            if (Server.StartsWith("sftp://"))
            {
                paths = Server.Substring(7).Split('/');
            } else 
            {
                paths = new [] { Server};
            }

            var serverName = paths[0];
            var workingDirectory = "/" + string.Join("/", paths.Skip(1));

            var connectionInfo = new ConnectionInfo(serverName,
                Username,
                new PasswordAuthenticationMethod(Username, Password),
                new PrivateKeyAuthenticationMethod("rsa.key"));
            
            var client = new SftpClient(connectionInfo);
            
            client.Connect();
            client.ChangeDirectory(workingDirectory);

            return client;
        }
        
        public override Task<List<string>> GetFileShares()
        {
            try
            {
                using (var client = GetSftpClient())
                {

                    var directories = new List<string>();
                    
                    foreach (var item in client.ListDirectory("."))
                    {
                        if (item.IsDirectory)
                        {
                            directories.Add(item.Name);
                        }
                    }

                    return Task.FromResult(directories);
                }
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
                using (var client = GetSftpClient())
                {

                    var directory = DefaultDatabase;
                    
                    if (!client.Exists(directory))
                    {
                        client.CreateDirectory(directory);
                    }

                    if (file != null && !string.IsNullOrEmpty(file.FileRootPath))
                    {
                        directory = CombinePath(DefaultDatabase, file.FileRootPath);
                        if (!Directory.Exists(directory))
                        {
                            client.CreateDirectory(directory);
                        }
                    }

                    if (file != null && path != EFlatFilePath.None)
                    {
                        directory = CombinePath(directory, file.GetPath(path));
                        if (!Directory.Exists(directory))
                        {
                            client.CreateDirectory(directory);
                        }
                    }
                    
                    client.Disconnect();
                    
                    return Task.FromResult(true);
                }
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

                using (var client = GetSftpClient())
                {
                    var sourceFile = client.ListDirectory(CombinePath(fullFromDirectory, fileName)).FirstOrDefault();

                    if (sourceFile == null)
                    {
                        return false;
                    }

                    // if there is already a file with the same name on the target directory, add a version number until a unique name is found.
                    while (client.Exists(CombinePath(fullToDirectory, newFileName)))
                    {
                        version++;
                        newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
                    }

                    sourceFile.MoveTo(CombinePath(fullToDirectory, newFileName));
                    return true;
                }

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
                using (var client = GetSftpClient())
                {
                    var fullDirectory = GetFullPath(file, path);
                    client.DeleteFile(CombinePath(fullDirectory, fileName));
                    return Task.FromResult(true);
                }
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

                using (var client = GetSftpClient())
                {

                    var fullDirectory = GetFullPath(file, path);
                    var directoryListing = client.ListDirectory(fullDirectory);
                    foreach (var directoryItem in directoryListing)
                    {
                        if (directoryItem.IsRegularFile &&
                            (string.IsNullOrEmpty(searchPattern) || FitsMask(directoryItem.Name, searchPattern)))
                        {
                            files.Add(new DexihFileProperties()
                            {
                                FileName = directoryItem.Name,
                                LastModified = directoryItem.LastWriteTime,
                                Length = directoryItem.Length
                            });
                        }
                    }

                    var newFiles = new DexihFiles(files.ToArray());
                    return Task.FromResult(newFiles);
                }
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
                using (var client = GetSftpClient())
                {
                    var directoryListing = client.ListDirectory(fullDirectory);
                    foreach (var directoryItem in directoryListing)
                    {
                        if (directoryItem.IsRegularFile)
                        {
                            files.Add(new DexihFileProperties()
                            {
                                FileName = directoryItem.Name,
                                LastModified = directoryItem.LastWriteTime,
                                Length = directoryItem.Length,
                                ContentType = ""
                            });
                        }
                    }

                    return Task.FromResult(files);
                }
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
                var client = GetSftpClient();
                var reader = client.OpenRead(CombinePath(fullDirectory, fileName));
                var ftpStream = new SftpStream(reader, client);
                return Task.FromResult<Stream>(ftpStream);
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

                await CreateDirectory(file, path);
                var client = GetSftpClient();
                var fullDirectory = GetFullPath(file, path);
                var reader = client.OpenWrite(CombinePath(fullDirectory, fileName));

                var ftpStream = new SftpStream(reader, client);
                return ftpStream;
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
                await CreateDirectory(file, path);
                var filePath = await FixFileName(file, path, fileName);

                using (var client = GetSftpClient())
                using (var newFile = client.OpenWrite(filePath))
                {
                    await stream.CopyToAsync(newFile);
                    await stream.FlushAsync();
                    return true;
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred saving file stream of file {fileName} at {path}.  {ex.Message}", ex);
            }
        }

        private Task<string> FixFileName(FlatFile file, EFlatFilePath path, string fileName)
         {
            var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
            var fileNameExtension = Path.GetExtension(fileName);
    
            var version = 0;
    
             var fullPath = GetFullPath(file, path);
    
    
             var newFileName = fileName;
             using (var client = GetSftpClient())
             {
                 while (client.Exists(CombinePath(fullPath, newFileName)))
                 {
                     version++;
                     newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
                 }
    
                 var filePath = CombinePath(fullPath, newFileName);
    
                 return Task.FromResult(filePath);
             }
         }

        public override Task<bool> TestFileConnection()
        {
            try
            {
                using (GetSftpClient())
                {
                    State = EConnectionState.Open;
                    return Task.FromResult(true);
                }
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
                var fullPath = CombinePath(DefaultDatabase, flatFile.FileRootPath ?? "");

                using (var client = GetSftpClient())
                {

                    var exists = client.Exists(fullPath);
                    return Task.FromResult(exists);
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Error occurred testing if a directory exists for flatfile {table.Name}.  {ex.Message}", ex);
            }
        }
    }
}
