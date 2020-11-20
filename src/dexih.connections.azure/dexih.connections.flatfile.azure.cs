using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Threading;
using dexih.transforms.Exceptions;
using dexih.functions.Query;
using dexih.transforms.File;

namespace dexih.connections.azure
{
    [Connection(
        ConnectionCategory = EConnectionCategory.File,
        Name = "Azure Storage Flat File", 
        Description = "Use flat files on an Azure Storage Blob.",
        DatabaseDescription = "Directory",
        ServerDescription = "Azure End Point",
        AllowsConnectionString = true,
        AllowsSql = false,
        AllowsFlatFiles = false,
        AllowsManagedConnection = false,
        AllowsSourceConnection = true,
        AllowsTargetConnection = true,
        AllowsUserPassword = true,
        AllowsWindowsAuth = false,
        RequiresDatabase = true
    )]
    public class ConnectionFlatFileAzureFile : ConnectionFlatFile
    {
        private CloudBlobClient _cloudBlobClient;
        private CloudBlobContainer _cloudBlobContainer;

        private CloudBlobClient CloudBlobClient
        {
            get
            {
                if (_cloudBlobClient == null)
                {
                    CloudStorageAccount storageAccount;
                    // Retrieve the storage account from the connection string.
                    if (UseConnectionString)
                        storageAccount = CloudStorageAccount.Parse(ConnectionString);
                    else if (string.IsNullOrEmpty(Username)) //no username, then use the development settings.
                        storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true;");
                    else
                        storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=" + Username + ";AccountKey=" + Password + ";BlobEndpoint=" + Server + ";TableEndpoint=" + Server + ";QueueEndpoint=" + Server + ";FileEndpoint=" + Server);

                    // Create the table client.
                    _cloudBlobClient = storageAccount.CreateCloudBlobClient();
                }

                return _cloudBlobClient;
            }
        }

        // private async Task<CloudBlobContainer> GetCloudBlobContainer(CancellationToken cancellationToken)
        // {
        //     if (_cloudBlobContainer == null)
        //     {
        //         _cloudBlobContainer = CloudBlobClient.GetContainerReference(DefaultDatabase);
        //         await _cloudBlobContainer.CreateIfNotExistsAsync();
        //     }
        //
        //     return _cloudBlobContainer;
        // }

        public override async Task<List<string>> GetFileShares(CancellationToken cancellationToken)
        {
            var fileShares = new List<string>();

            // Create the table client.
            var list = new List<CloudBlobContainer>();
            try
            {
                BlobContinuationToken continuationToken = null;
                do
                {
                    var shares = await CloudBlobClient.ListContainersSegmentedAsync(continuationToken);
                    continuationToken = shares.ContinuationToken;
                    list.AddRange(shares.Results);

                } while (continuationToken != null);

            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Failed get the azure file containers on {Server}.  {ex.Message}", ex);
            }

            foreach (var share in list)
            {
                fileShares.Add(share.Name);
            }
            return fileShares;
        }

        private async Task<CloudBlobDirectory> GetDatabaseDirectory()
        {
            try
            {
                if (_cloudBlobContainer == null)
                {
                    _cloudBlobContainer = CloudBlobClient.GetContainerReference(DefaultDatabase.ToLower());
                    await _cloudBlobContainer.CreateIfNotExistsAsync();
                }

                return _cloudBlobContainer.GetDirectoryReference("");
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to get the root directory {DefaultDatabase}.  {ex.Message}", ex);
            }
        }

        private async Task<CloudBlobDirectory> GetFileDirectory(FlatFile file)
        {
            try
            {
                var fileShare = await GetDatabaseDirectory();

                if (file != null && !string.IsNullOrEmpty(file.FileRootPath))
                {
                    fileShare = fileShare.GetDirectoryReference(file.FileRootPath);
                    await fileShare.Container.CreateIfNotExistsAsync();
                }

                return fileShare;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to get the directory for file  {file.Name}.  {ex.Message}", ex);
            }
        }
        
        public override string GetFullPath(FlatFile file, EFlatFilePath path)
        {
            throw new ConnectionException("File paths are not available on Azure connections.");
        }


        public override async Task<bool> CreateDirectory(FlatFile file, EFlatFilePath path, CancellationToken cancellationToken)
        {
            try
            {
                var directory = await GetDatabaseDirectory();

                var cloudFileDirectory = directory;

                if (file != null && !string.IsNullOrEmpty(file.FileRootPath))
                {
                    cloudFileDirectory = directory.GetDirectoryReference(file.FileRootPath);
                    await cloudFileDirectory.Container.CreateIfNotExistsAsync();
                }

                if(file != null &&  path != EFlatFilePath.None)
                {
                    cloudFileDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                    await cloudFileDirectory.Container.CreateIfNotExistsAsync();
                }

                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to create the directory for file  {file.Name} path {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<bool> MoveFile(FlatFile file, EFlatFilePath fromDirectory, EFlatFilePath toDirectory, string fileName, CancellationToken cancellationToken)
        {
            try
            {
                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var fileNameExtension = Path.GetExtension(fileName);
                var version = 0;

                var cloudFileDirectory = await GetFileDirectory(file);
                var cloudFromDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(fromDirectory));
                var cloudToDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(toDirectory));

                CloudBlob sourceFile = cloudFromDirectory.GetBlockBlobReference(fileName);
                CloudBlob targetFile = cloudToDirectory.GetBlockBlobReference(fileName);

                while (await targetFile.ExistsAsync())
                {
                    version++;
                    var newFileName = fileNameWithoutExtension + "_" + version + fileNameExtension;
                    targetFile = cloudToDirectory.GetBlockBlobReference(newFileName);
                }
                await targetFile.StartCopyAsync(sourceFile.Uri);
                await sourceFile.DeleteAsync();

                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to move file {file.Name} filename {Filename} from {fromDirectory} to {toDirectory}.  {ex.Message}", ex);
            }
        }

        public override async Task<bool> DeleteFile(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
            try
            {
                var cloudFileDirectory = await GetFileDirectory(file);
                var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                CloudBlob cloudFile = cloudSubDirectory.GetBlockBlobReference(fileName);
                await cloudFile.DeleteAsync();
                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to delete file {file.Name} filename {Filename} from {path}.  {ex.Message}", ex);
            }
        }

        public override async IAsyncEnumerable<FileProperties> GetFileEnumerator(FlatFile file, EFlatFilePath path,
            string searchPattern, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var cloudFileDirectory = await GetFileDirectory(file);
            var pathString = file.GetPath(path);
            var pathLength = pathString.Length + 1;
            var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(pathString);

            BlobContinuationToken continuationToken = null;
            var list = new List<IListBlobItem>();
            do
            {
                var filesList = await cloudSubDirectory.ListBlobsSegmentedAsync(false, BlobListingDetails.None, 500,
                    continuationToken, null, null, cancellationToken);
                continuationToken = filesList.ContinuationToken;
                list.AddRange(filesList.Results);

            } while (continuationToken != null);

            foreach (var listBlobItem in list)
            {
                var cloudFile = (CloudBlob) listBlobItem;
                await cloudFile.FetchAttributesAsync();
                var fileName = cloudFile.Name.Substring(pathLength);
                if (string.IsNullOrEmpty(searchPattern) || FitsMask(file.Name, searchPattern))
                {
                    var properties = new FileProperties()
                    {
                        FileName = fileName, 
                        LastModified = cloudFile.Properties.LastModified.Value.DateTime,
                        Length = cloudFile.Properties.Length
                    };
                    yield return properties;
                }
            }
        }

        // public override async Task<List<DexihFileProperties>> GetFileList(FlatFile file, EFlatFilePath path, CancellationToken cancellationToken)
        // {
        //     try
        //     {
        //         var files = new List<DexihFileProperties>();
        //
        //         var cloudFileDirectory = await GetFileDirectory(file, cancellationToken);
        //
        //         var pathstring = file.GetPath(path);
        //         var pathlength = pathstring.Length + 1;
        //         var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(pathstring);
        //
        //         BlobContinuationToken continuationToken = null;
        //         var list = new List<IListBlobItem>();
        //         do
        //         {
        //             var filesList = await cloudSubDirectory.ListBlobsSegmentedAsync(true, BlobListingDetails.None, 500, continuationToken, null, null);
        //             continuationToken = filesList.ContinuationToken;
        //             list.AddRange(filesList.Results);
        //
        //         } while (continuationToken != null);
        //
        //         foreach (CloudBlob cloudFile in list)
        //         {
        //             await cloudFile.FetchAttributesAsync();
        //             var fileName = cloudFile.Name.Substring(pathlength);
        //             files.Add(new DexihFileProperties() { FileName = fileName, LastModified = cloudFile.Properties.LastModified.Value.DateTime, Length = cloudFile.Properties.Length, ContentType = cloudFile.Properties.ContentType });
        //         }
        //         return files;
        //     }
        //     catch (Exception ex)
        //     {
        //         throw new ConnectionException($"Failed get filelist {file.Name} files in path {path}.  {ex.Message}", ex);
        //     }
        // }

        public override async Task<Stream> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
            try
            {
                var cloudFileDirectory = await GetFileDirectory(file);
                var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                var stream = await cloudSubDirectory.GetBlockBlobReference(fileName).OpenReadAsync();
                return stream;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed read the file {file.Name} name {Filename} in path {path}.  {ex.Message}", ex);
            }

        }

        public override async Task<Stream> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
            try
            {
                var cloudFileDirectory = await GetFileDirectory(file);
                var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                Stream stream = await cloudSubDirectory.GetBlockBlobReference(fileName).OpenWriteAsync();
                return stream;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed write the file {file.Name} name {Filename} in path {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<bool> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream fileStream, CancellationToken cancellationToken)
        {
            try
            {
				var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var fileNameExtension = Path.GetExtension(fileName);
                var version = 0;

                var cloudFileDirectory = await GetFileDirectory(file);
                var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                var cloudFile = cloudSubDirectory.GetBlockBlobReference(fileName);

                while (await cloudFile.ExistsAsync())
                {
                    version++;
                    var newFileName = fileNameWithoutExtension + "_" + version + fileNameExtension;
                    cloudFile = cloudSubDirectory.GetBlockBlobReference(newFileName);
                }

                await cloudFile.UploadFromStreamAsync(fileStream);
                fileStream.Dispose();
                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed save the file {file.Name} name {Filename} in path {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<bool> TestFileConnection(CancellationToken cancellationToken)
        {
            try
            {
                var connection = CloudBlobClient;
                await connection.GetServicePropertiesAsync();
                State = EConnectionState.Open;
                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The azure file connection test failed.  {ex.Message}", ex);
            }
        }


        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
				var flatFile = (FlatFile)table;

				var getDatabaseDirectory = await GetDatabaseDirectory();
                var cloudFileDirectory = getDatabaseDirectory.GetDirectoryReference(flatFile.FileRootPath);

                var exists = await cloudFileDirectory.Container.ExistsAsync();
                return exists;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to check the directory exists for {table.Name}.  {ex.Message}", ex);
            }
        }
    }
}
