using dexih.transforms;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using dexih.functions;
using dexih.transforms.Exceptions;
using dexih.transforms.File;

namespace dexih.connections.github
{
    [Connection(
        ConnectionCategory = EConnectionCategory.File,
        Name = "GitHub Flat File", 
        Description = "Flat files from a github repository.  [Create a GitHub oAuth Token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) to improve rate limits.",
        DatabaseDescription = "Sub Directory",
        ServerDescription = "Github Repository.",
        ServerHelp = "Use format owner/repo (e.g. DataExperts/TestRepository",
        AllowsConnectionString = false,
        AllowsSql = false,
        AllowsFlatFiles = true,
        AllowsManagedConnection = false,
        AllowsSourceConnection = true,
        AllowsTargetConnection = true,
        AllowsUserPassword = false,
        AllowsToken = true,
        AllowsWindowsAuth = false,
        RequiresDatabase = false,
        RequiresLocalStorage = false
    )]
    public class ConnectionGitHubFlatFile : ConnectionFlatFile
    {
        private HttpClient _httpClient;
        private HttpClient HttpClient => _httpClient ??= new HttpClient();

        public override void Dispose()
        {
            _httpClient?.Dispose();
            base.Dispose();
        }

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

        private Uri BaseUri => new Uri($"https://api.github.com/repos/{GetServer()}/");
        private Uri RawUri => new Uri($"https://raw.githubusercontent.com/{GetServer()}/master");

        private string GetServer()
        {
            var server = Server;
            if (string.IsNullOrWhiteSpace(server))
            {
                throw new ConnectionException("The github repository is not specified.");
            }

            if (server.StartsWith("https://github.com/"))
            {
                server = server.Substring(19);
            }

            var ownerRepo = server.Split('/');

            if (ownerRepo.Length != 2)
            {
                throw new ConnectionException($"The github repository {Server} is not in the format owner/repo.");
            }

            return ownerRepo[0].Trim() + "/" + ownerRepo[1].Trim();
        }
        
        public async Task<JsonDocument> GitHubAction(string uri, HttpMethod method, object data, CancellationToken cancellationToken)
        {
            HttpRequestMessage request;
            if (data != null)
            {
                var json = JsonSerializer.Serialize(data);
                var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");
                request = new HttpRequestMessage(method, new Uri(BaseUri, uri))
                {
                    Content = jsonContent
                };
            }
            else
            {
                request = new HttpRequestMessage(method, new Uri(BaseUri, uri));
            }

            request.Headers.UserAgent.Add(new ProductInfoHeaderValue("DexihRemote", "1.0"));
            request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

            if (!string.IsNullOrEmpty(Password))
            {
                request.Headers.Authorization = new AuthenticationHeaderValue("Token", Password);
            }

            var response = await HttpClient.SendAsync(request, cancellationToken);

            if (response.IsSuccessStatusCode)
            {
                var jsonDocument = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(),
                    cancellationToken: cancellationToken);
                return jsonDocument;
            }

            if (response.Content == null)
            {
                throw new ConnectionException($"There was an issue connecting to github: {response.ReasonPhrase}.");
            }

            var error = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(), cancellationToken: cancellationToken);

            if (error != null)
            {
                if(error.RootElement.TryGetProperty("message", out var message))
                {
                    throw new ConnectionException($"There was an issue connecting with github: Message: {message}");
                }
            }
            
            throw new ConnectionException($"There was an issue connecting to github: {response.ReasonPhrase}.");
        }

        private async Task<List<string>> GetDirectories(string directory, CancellationToken cancellationToken)
        {
            try
            {
                var directories = await GitHubAction("contents/" + directory, HttpMethod.Get, null, cancellationToken);

                var values = new List<string>();

                foreach (var item in directories.RootElement.EnumerateArray())
                {
                    if (item.TryGetProperty("type", out var type))
                    {
                        if (type.GetString() == "dir")
                        {
                            if (item.TryGetProperty("name", out var value))
                            {
                                values.Add(value.GetString());
                            }
                            else
                            {
                                throw new ConnectionException($"Github error occurred getting directories from {Server}.  The \"name\" property could not be found.", new Exception("Full response: " + directories));
                            }
                            
                        }
                    }
                    else
                    {
                        throw new ConnectionException($"Github error occurred getting directories from {Server}.  The \"type\" property could not be found.", new Exception("Full response: " + directories));
                    }
                }

                return values;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Github error occurred getting directories from {Server}.  {ex.Message}", ex);
            }
        }

        public override Task<List<string>> GetFileShares(CancellationToken cancellationToken)
        {
            return GetDirectories(DefaultDatabase, cancellationToken);
        }
        
        public override Task<bool> CreateDirectory(FlatFile file, EFlatFilePath path, CancellationToken cancellationToken)
        {
            // github doesn't store empty directories
            return Task.FromResult(true);
        }

        public override Task<bool> MoveFile(FlatFile file, EFlatFilePath fromPath, EFlatFilePath toPath, string fileName, CancellationToken cancellationToken)
        {
            throw new ConnectionException($"Moving files not supported");
        }

        public override Task<bool> DeleteFile(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
            throw new ConnectionException($"Deleting files not supported");
        }

        public override async IAsyncEnumerable<FileProperties> GetFileEnumerator(FlatFile file, EFlatFilePath path, string searchPattern, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var fullDirectory = GetFullPath(file, path);

            var gitHubFiles = await GitHubAction("contents/" + fullDirectory, HttpMethod.Get, null, cancellationToken);

            foreach (var item in gitHubFiles.RootElement.EnumerateArray())
            {
                if (item.TryGetProperty("type", out var type))
                {
                    if (type.GetString() == "file")
                    {
                        var name = item.GetProperty("name").GetString();
                        var filePath = item.GetProperty("path").GetString();
                        if (string.IsNullOrEmpty(searchPattern) || FitsMask(name, searchPattern))
                        {
                            var properties = new FileProperties()
                            {
                                FileName = item.GetProperty("name").GetString(),
                                Length = item.GetProperty("size").GetInt32(),
                                ContentType = ""
                            };

                            var hasLoaded = false;
                            properties.LoadAttributes = async () => 
                            {
                                if (!hasLoaded)
                                {
                                    hasLoaded = true;
                                    var commit = await GitHubAction("commits?page=1&per_page=1&path=" + filePath,
                                        HttpMethod.Get, null, cancellationToken);
                                    var commitItem = commit.RootElement.EnumerateArray().First();
                                    var date = commitItem.GetProperty("commit").GetProperty("committer")
                                        .GetProperty("date").GetDateTime();
                                    properties.LastModified = date;
                                    var owner = commitItem.GetProperty("commit").GetProperty("committer")
                                        .GetProperty("name").GetString();
                                    properties.Owner = owner;
                                }
                            };
                            
                            yield return properties;
                        }
                    }
                }
                else
                {
                    throw new ConnectionException(
                        $"Github error occurred getting directories from {Server}.  The \"type\" property could not be found.",
                        new Exception("Full response: " + gitHubFiles));
                }
            }        
        }
        
        public override async Task<Stream> GetReadFileStream(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
            try
            {
                var fullDirectory = GetFullPath(file, path);

                var request = new HttpRequestMessage(HttpMethod.Get, new Uri(RawUri + fullDirectory + "/" + fileName));
                request.Headers.UserAgent.Add(new ProductInfoHeaderValue("DexihRemote", "1.0"));
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                if (!string.IsNullOrEmpty(Password))
                {
                    request.Headers.Authorization = new AuthenticationHeaderValue("Token", Password);
                }

                var response = await HttpClient.SendAsync(request, cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    return await response.Content.ReadAsStreamAsync();
                }

                var responseString = await response.Content.ReadAsStringAsync();
                
                throw new ConnectionException($"Error reading file {fileName}, reason: {response.ReasonPhrase}.", new Exception($"Full response: {responseString}"));
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred reading file {fileName} at {path}.  {ex.Message}", ex);
            }
        }

        public override  Task<Stream> GetWriteFileStream(FlatFile file, EFlatFilePath path, string fileName, CancellationToken cancellationToken)
        {
            throw new ConnectionException($"Writing files not supported");
        }

        public override  Task<bool> SaveFileStream(FlatFile file, EFlatFilePath path, string fileName, Stream stream, CancellationToken cancellationToken)
        {
            throw new ConnectionException($"Writing files not supported");
        }

        public override async Task<bool> TestFileConnection(CancellationToken cancellationToken)
        {
            try
            {
                var result = await GitHubAction("contents", HttpMethod.Get, null, cancellationToken);

                if (result != null)
                {
                    return true;
                }

                return false;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Error occurred testing if file connection {Server}.  {ex.Message}", ex);
            }
        }

        public override async Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
				var flatFile = (FlatFile)table;
                var fullPath = CombinePath(DefaultDatabase, flatFile.FileRootPath ?? "");

                var directories = await GetDirectories(fullPath, cancellationToken);
                return directories.Contains(table.Name);
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Error occurred testing if a directory exists for flat file {table.Name}.  {ex.Message}", ex);
            }
        }
    }
}
