using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using dexih.functions;
using dexih.transforms;
using System.Data.Common;
using System.Threading;
using dexih.connections.flatfile;
using System.Text.RegularExpressions;
using static dexih.connections.flatfile.FlatFile;
using System.Linq;
using dexih.transforms.Exceptions;
using dexih.functions.Query;

namespace dexih.connections.azure
{
    public class ConnectionFlatFileAzureFile : ConnectionFlatFile
    {
        public CloudBlobClient CloudBlobClient;
        public CloudBlobContainer CloudBlobContainer;

        private CloudBlobClient _CloudBlobClient
        {
            get
            {
                if (CloudBlobClient == null)
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
                    CloudBlobClient = storageAccount.CreateCloudBlobClient();
                }

                return CloudBlobClient;
            }
        }

        private async Task<CloudBlobContainer> GetCloudBlobContainer()
        {
            if (CloudBlobContainer == null)
            {
                CloudBlobContainer = _CloudBlobClient.GetContainerReference(DefaultDatabase);
                await CloudBlobContainer.CreateIfNotExistsAsync();
            }

            return CloudBlobContainer;
        }

        public override async Task<List<string>> GetFileShares(string serverName, string userName, string password)
        {
            var fileShares = new List<string>();

            // Create the table client.
            var list = new List<CloudBlobContainer>();
            try
            {
                BlobContinuationToken continuationToken = null;
                do
                {
                    var shares = await _CloudBlobClient.ListContainersSegmentedAsync(continuationToken);
                    continuationToken = shares.ContinuationToken;
                    list.AddRange(shares.Results);

                } while (continuationToken != null);

            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Failed get the azure file containers on {serverName}.  {ex.Message}", ex);
            }

            foreach (var share in list)
            {
                fileShares.Add(share.Name);
            }
            return fileShares;
        }

        public async Task<CloudBlobDirectory> GetDatabaseDirectory()
        {
            try
            {
                if (CloudBlobContainer == null)
                {
                    CloudBlobContainer = _CloudBlobClient.GetContainerReference(DefaultDatabase.ToLower());
                    await CloudBlobContainer.CreateIfNotExistsAsync();
                }

                return CloudBlobContainer.GetDirectoryReference("");
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed to get the root directory {DefaultDatabase}.  {ex.Message}", ex);
            }
        }

        public async Task<CloudBlobDirectory> GetFileDirectory(FlatFile file)
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


        public override async Task<bool> CreateDirectory(FlatFile file, EFlatFilePath path)
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

        public override async Task<bool> MoveFile(FlatFile file, EFlatFilePath fromDirectory, EFlatFilePath toDirectory, string fileName)
        {
            try
            {
                var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var fileNameExtension = Path.GetExtension(fileName);
                var version = 0;
                string newFileName;

                var cloudFileDirectory = await GetFileDirectory(file);
                var cloudFromDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(fromDirectory));
                var cloudToDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(toDirectory));

                CloudBlob sourceFile = cloudFromDirectory.GetBlockBlobReference(fileName);
                CloudBlob targetFile = cloudToDirectory.GetBlockBlobReference(fileName);

                while (await targetFile.ExistsAsync())
                {
                    version++;
                    newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
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

        public override async Task<bool> DeleteFile(FlatFile file, EFlatFilePath path, string fileName)
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

<<<<<<< HEAD
        public override async Task<DexihFiles> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern)
=======
        public override async Task<ReturnValue<DexihFiles>> GetFileEnumerator(string mainDirectory, string subDirectory, string searchPattern)
>>>>>>> cb9c297fbbe8f6a4412b32cfd099296bcac701d6
        {
            try
            {
                var files = new List<DexihFileProperties>();

                var cloudFileDirectory = await GetFileDirectory(file);
                var pathstring = file.GetPath(path);
                var pathlength = pathstring.Length + 1;
                var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(pathstring);

                BlobContinuationToken continuationToken = null;
                var list = new List<IListBlobItem>();
                do
                {
                    var filesList = await cloudSubDirectory.ListBlobsSegmentedAsync(false, BlobListingDetails.None, 500, continuationToken, null, null);
                    continuationToken = filesList.ContinuationToken;
                    list.AddRange(filesList.Results);

                } while (continuationToken != null);

                foreach (CloudBlob cloudFile in list)
                {
<<<<<<< HEAD
                    await cloudFile.FetchAttributesAsync();
                    var fileName = cloudFile.Name.Substring(pathlength);
                    if (string.IsNullOrEmpty(searchPattern) || FitsMask(file.Name, searchPattern))
                    {
                        files.Add(new DexihFileProperties() { FileName = fileName, LastModified = cloudFile.Properties.LastModified.Value.DateTime, Length = cloudFile.Properties.Length });
=======
                    await file.FetchAttributesAsync();
                    if (string.IsNullOrEmpty(searchPattern) || FitsMask(file.Name, searchPattern))
                    {
                        files.Add(new DexihFileProperties() { FileName = file.Name, LastModified = file.Properties.LastModified.Value.DateTime, Length = file.Properties.Length });
>>>>>>> cb9c297fbbe8f6a4412b32cfd099296bcac701d6
                    }
                }
                var newFiles = new DexihFiles(files.ToArray());
                return newFiles;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed get file {file.Name} files in path {path} with pattern {searchPattern}.  {ex.Message}", ex);
            }
        }

        private bool FitsMask(string fileName, string fileMask)
        {
<<<<<<< HEAD
            var pattern =
=======
            string pattern =
>>>>>>> cb9c297fbbe8f6a4412b32cfd099296bcac701d6
                 '^' +
                 Regex.Escape(fileMask.Replace(".", "__DOT__")
                                 .Replace("*", "__STAR__")
                                 .Replace("?", "__QM__"))
                     .Replace("__DOT__", "[.]")
                     .Replace("__STAR__", ".*")
                     .Replace("__QM__", ".")
                 + '$';
            return new Regex(pattern, RegexOptions.IgnoreCase).IsMatch(fileName);
        }

<<<<<<< HEAD
        public override async Task<List<DexihFileProperties>> GetFileList(FlatFile file, EFlatFilePath path)
=======
        public override async Task<ReturnValue<List<DexihFileProperties>>> GetFileList(string mainDirectory, string subDirectory)
>>>>>>> cb9c297fbbe8f6a4412b32cfd099296bcac701d6
        {
            try
            {
                var files = new List<DexihFileProperties>();

                var cloudFileDirectory = await GetFileDirectory(file);

                var pathstring = file.GetPath(path);
                var pathlength = pathstring.Length + 1;
                var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(pathstring);

                BlobContinuationToken continuationToken = null;
                var list = new List<IListBlobItem>();
                do
                {
                    var filesList = await cloudSubDirectory.ListBlobsSegmentedAsync(true, BlobListingDetails.None, 500, continuationToken, null, null);
                    continuationToken = filesList.ContinuationToken;
                    list.AddRange(filesList.Results);

                } while (continuationToken != null);

                foreach (CloudBlob cloudFile in list)
                {
                    await cloudFile.FetchAttributesAsync();
                    var fileName = cloudFile.Name.Substring(pathlength);
                    files.Add(new DexihFileProperties() { FileName = fileName, LastModified = cloudFile.Properties.LastModified.Value.DateTime, Length = cloudFile.Properties.Length, ContentType = cloudFile.Properties.ContentType });
                }
                return files;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Failed get filelist {file.Name} files in path {path}.  {ex.Message}", ex);
            }
        }

        public override async Task<Stream> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName)
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

        public override async Task<Stream> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName)
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

        public override async Task<bool> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream fileStream)
        {
            try
            {
				var fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var fileNameExtension = Path.GetExtension(fileName);
                var version = 0;
                string newFileName;

                var cloudFileDirectory = await GetFileDirectory(file);
                var cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                var cloudFile = cloudSubDirectory.GetBlockBlobReference(fileName);

                while (await cloudFile.ExistsAsync())
                {
                    version++;
                    newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
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

        public override async Task<bool> TestFileConnection()
        {
            try
            {
                var connection = _CloudBlobClient;
                var serviceProperties = await connection.GetServicePropertiesAsync();
                State = EConnectionState.Open;
                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"The azure file connection test failed.  {ex.Message}", ex);
            }
        }


        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken)
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
