using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.File;
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

namespace dexih.connections.azure
{
    public class ConnectionFlatFileAzureFile : ConnectionFlatFile
    {
        public CloudFileClient CloudFileClient;
        public CloudFileShare CloudFileShare;

        private CloudFileClient GetCloudFileClient()
        {
            if (CloudFileClient == null)
            {
                CloudStorageAccount storageAccount;
                // Retrieve the storage account from the connection string.
                if(UseConnectionString)
                    storageAccount = CloudStorageAccount.Parse(ConnectionString);
                else if (string.IsNullOrEmpty(Username)) //no username, then use the development settings.
                    storageAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
                else
                    storageAccount = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=" + Username + ";AccountKey=" + Password + ";BlobEndpoint=" + Server + ";TableEndpoint=" + Server + ";QueueEndpoint=" + Server + ";FileEndpoint=" + Server);

                // Create the table client.
                CloudFileClient = storageAccount.CreateCloudFileClient();
            }

            return CloudFileClient;
        }

        private async Task<CloudFileShare> GetCloudFileShare()
        {
            if (CloudFileShare == null)
            {
                CloudFileShare = GetCloudFileClient().GetShareReference(DefaultDatabase);
                await CloudFileShare.CreateIfNotExistsAsync();
            }

            return CloudFileShare;
        }

        public override async Task<ReturnValue<List<string>>> GetFileShares(string serverName, string userName, string password)
        {
            List<string> fileShares = new List<string>();

            // Create the table client.
            List<CloudFileShare> list = new List<CloudFileShare>();
            try
            {
                FileContinuationToken continuationToken = null;
                do
                {
                    var shares = await GetCloudFileClient().ListSharesSegmentedAsync(continuationToken);
                    continuationToken = shares.ContinuationToken;
                    list.AddRange(shares.Results);

                } while (continuationToken != null);

            }
            catch(Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The following error occurred retrieving the Azure File shares: " + ex.Message, ex, null);
            }

            foreach (CloudFileShare share in list)
            {
                fileShares.Add(share.Name);
            }
            return new ReturnValue<List<string>>(true, "", null, fileShares);
        }

        public async Task<ReturnValue<CloudFileDirectory>> GetDatabaseDirectory()
        {
            try
            {
                if (CloudFileShare == null)
                {
                    CloudFileShare = GetCloudFileClient().GetShareReference(DefaultDatabase.ToLower());
                    await CloudFileShare.CreateIfNotExistsAsync();
                }

                return new ReturnValue<CloudFileDirectory>(true, CloudFileShare.GetRootDirectoryReference());
            }
            catch (Exception ex)
            {
                return new ReturnValue<CloudFileDirectory>(false, "There was an issue getting the root directory - " + DefaultDatabase + ", message: " + ex.Message, ex);
            }
        }

        public async Task<ReturnValue<CloudFileDirectory>> GetFileDirectory(FlatFile file)
        {
            try
            {
                var getDatabaseDirectory = await GetDatabaseDirectory();
                if(!getDatabaseDirectory.Success)
                {
                    return getDatabaseDirectory;
                }

                var fileShare = getDatabaseDirectory.Value;

                if(file != null && string.IsNullOrEmpty(file.FileRootPath))
                {
                    fileShare = fileShare.GetDirectoryReference(file.FileRootPath);
                    await fileShare.CreateIfNotExistsAsync();
                }

                return new ReturnValue<CloudFileDirectory>(true, fileShare);
            }
            catch (Exception ex)
            {
                return new ReturnValue<CloudFileDirectory>(false, "There was an issue getting the root directory - " + DefaultDatabase + ", message: " + ex.Message, ex);
            }
        }


        public override async Task<ReturnValue> CreateDirectory(FlatFile file, EFlatFilePath path)
        {
            try
            {
                var directory = await GetDatabaseDirectory();
                if (!directory.Success)
                    return directory;

                CloudFileDirectory cloudFileDirectory = directory.Value;

                if (file != null && string.IsNullOrEmpty(file.FileRootPath))
                {
                    cloudFileDirectory = directory.Value.GetDirectoryReference(file.FileRootPath);
                    await cloudFileDirectory.CreateIfNotExistsAsync();
                }

                if(file != null &&  path != EFlatFilePath.none)
                {
                    cloudFileDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                    await cloudFileDirectory.CreateIfNotExistsAsync();
                }

                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred creating Azure directory: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> MoveFile(FlatFile file, EFlatFilePath fromDirectory, EFlatFilePath toDirectory, string fileName)
        {
            try
            {
                string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                string fileNameExtension = Path.GetExtension(fileName);
                int version = 0;
                string newFileName;

                var getFileDirectory = await GetFileDirectory(file);
                if (!getFileDirectory.Success)
                    return getFileDirectory;

                CloudFileDirectory cloudFileDirectory = getFileDirectory.Value;
                CloudFileDirectory cloudFromDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(fromDirectory));
                CloudFileDirectory cloudToDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(toDirectory));

                CloudFile sourceFile = cloudFromDirectory.GetFileReference(fileName);
                CloudFile targetFile = cloudToDirectory.GetFileReference(fileName);

                while (await targetFile.ExistsAsync())
                {
                    version++;
                    newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
                    targetFile = cloudToDirectory.GetFileReference(newFileName);
                }
                await targetFile.StartCopyAsync(sourceFile);
                await sourceFile.DeleteAsync();

                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred moving Azure file: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> DeleteFile(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                var getFileDirectory = await GetFileDirectory(file);
                if (!getFileDirectory.Success)
                    return getFileDirectory;

                CloudFileDirectory cloudFileDirectory = getFileDirectory.Value;
                CloudFileDirectory cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                CloudFile cloudFile = cloudSubDirectory.GetFileReference(fileName);
                await cloudFile.DeleteAsync();
                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred deleting Azure file: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<DexihFiles>> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern)
        {
            try
            {
                List<DexihFileProperties> files = new List<DexihFileProperties>();

                var getFileDirectory = await GetFileDirectory(file);
                if (!getFileDirectory.Success)
                    return new ReturnValue<DexihFiles>(getFileDirectory);

                CloudFileDirectory cloudFileDirectory = getFileDirectory.Value;
                CloudFileDirectory cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));

                FileContinuationToken continuationToken = null;
                List<IListFileItem> list = new List<IListFileItem>();
                do
                {
                    var filesList = await cloudSubDirectory.ListFilesAndDirectoriesSegmentedAsync(continuationToken);
                    continuationToken = filesList.ContinuationToken;
                    list.AddRange(filesList.Results);

                } while (continuationToken != null);

                foreach (CloudFile cloudFile in list)
                {
                    await cloudFile.FetchAttributesAsync();
                    if (string.IsNullOrEmpty(searchPattern) || FitsMask(file.Name, searchPattern))
                    {
                        files.Add(new DexihFileProperties() { FileName = file.Name, LastModified = cloudFile.Properties.LastModified.Value.DateTime, Length = cloudFile.Properties.Length });
                    }
                }
                DexihFiles newFiles = new DexihFiles(files.ToArray());
                return new ReturnValue<DexihFiles>(true, "", null, newFiles);
            }
            catch (Exception ex)
            {
                return new ReturnValue<DexihFiles>(false, "The following error occurred retriving Azure file list: " + ex.Message, ex, null);
            }
        }

        private bool FitsMask(string fileName, string fileMask)
        {
            string pattern =
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

        public override async Task<ReturnValue<List<DexihFileProperties>>> GetFileList(FlatFile file, EFlatFilePath path)
        {
            try
            {
                List<DexihFileProperties> files = new List<DexihFileProperties>();

                var getFileDirectory = await GetFileDirectory(file);
                if (!getFileDirectory.Success)
                    return new ReturnValue<List<DexihFileProperties>>(getFileDirectory);

                CloudFileDirectory cloudFileDirectory = getFileDirectory.Value;
                CloudFileDirectory cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));

                FileContinuationToken continuationToken = null;
                List<IListFileItem> list = new List<IListFileItem>();
                do
                {
                    var filesList = await cloudSubDirectory.ListFilesAndDirectoriesSegmentedAsync(continuationToken);
                    continuationToken = filesList.ContinuationToken;
                    list.AddRange(filesList.Results);

                } while (continuationToken != null);

                foreach (CloudFile cloudFile in list)
                {
                    await cloudFile.FetchAttributesAsync();
                    files.Add(new DexihFileProperties() { FileName = file.Name, LastModified = cloudFile.Properties.LastModified.Value.DateTime, Length = cloudFile.Properties.Length, ContentType = cloudFile.Properties.ContentType });
                }
                return new ReturnValue<List<DexihFileProperties>>(true, "", null, files);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<DexihFileProperties>>(false, "The following error occurred retriving Azure file list: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Stream>> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                var getFileDirectory = await GetFileDirectory(file);
                if (!getFileDirectory.Success)
                    return new ReturnValue<Stream>(getFileDirectory);

                CloudFileDirectory cloudFileDirectory = getFileDirectory.Value;
                CloudFileDirectory cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                Stream reader2 = await cloudSubDirectory.GetFileReference(fileName).OpenReadAsync();
                return new ReturnValue<Stream>(true, "", null, reader2);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Stream>(false, "The following error occurred retriving Azure filestream: " + ex.Message, ex, null);
            }

        }

        public override async Task<ReturnValue<Stream>> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName)
        {
            try
            {
                var getFileDirectory = await GetFileDirectory(file);
                if (!getFileDirectory.Success)
                    return new ReturnValue<Stream>(getFileDirectory);

                CloudFileDirectory cloudFileDirectory = getFileDirectory.Value;
                CloudFileDirectory cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                Stream reader2 = await cloudSubDirectory.GetFileReference(fileName).OpenWriteAsync(1000);
                return new ReturnValue<Stream>(true, "", null, reader2);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Stream>(false, "The following error occurred writing to Azure filestream: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream fileStream)
        {
            try
            {
				string fileNameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                string fileNameExtension = Path.GetExtension(fileName);
                int version = 0;
                string newFileName;

                var getFileDirectory = await GetFileDirectory(file);
                if (!getFileDirectory.Success)
                    return new ReturnValue<DexihFiles>(getFileDirectory);

                CloudFileDirectory cloudFileDirectory = getFileDirectory.Value;
                CloudFileDirectory cloudSubDirectory = cloudFileDirectory.GetDirectoryReference(file.GetPath(path));
                CloudFile cloudFile = cloudSubDirectory.GetFileReference(fileName);

                while (await cloudFile.ExistsAsync())
                {
                    version++;
                    newFileName = fileNameWithoutExtension + "_" + version.ToString() + fileNameExtension;
                    cloudFile = cloudSubDirectory.GetFileReference(newFileName);
                }

                await cloudFile.UploadFromStreamAsync(fileStream);
                fileStream.Dispose();
                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred writing to Azure filestream: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue> TestFileConnection()
        {
            try
            {
                CloudFileClient connection = GetCloudFileClient();
                var serviceProperties = await connection.GetServicePropertiesAsync();
                State = EConnectionState.Open;
                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred opening the Azure file connection: " + ex.Message, ex);
            }
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
            try
            {
				FlatFile flatFile = (FlatFile)table;

				var getDatabaseDirectory = await GetDatabaseDirectory();
                if (!getDatabaseDirectory.Success)
                    return new ReturnValue<bool>(getDatabaseDirectory);

				CloudFileDirectory cloudFileDirectory = getDatabaseDirectory.Value.GetDirectoryReference(flatFile.FileRootPath);

                var exists = await cloudFileDirectory.ExistsAsync();
                return new ReturnValue<bool>(true, exists);
            }
            catch (Exception ex)
            {
                return new ReturnValue<bool>(false, "The following error occurred testing if a directory exists: " + ex.Message, ex);
            }
        }
    }
}
