﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.transforms;
using dexih.functions;
using System.Data.Common;
 using System.Linq;
 using System.Net.Http;
 using System.Net.Http.Headers;
 using System.Text.Json;
using System.Threading;
using dexih.transforms.Exceptions;
using dexih.functions.Query;

namespace dexih.connections.dexih
{
    [Connection(
        ConnectionCategory = EConnectionCategory.Hub,
        Name = "Data Experts Integration Hub", 
        Description = "Connects to another hub in an instance of the Data Experts Information Hub.  Note, tables must be shared in the hub to be imported.",
        DatabaseDescription = "Hub Name",
        ServerDescription = "Integration Hub Url",
        AllowsConnectionString = false,
        AllowsSql = false,
        AllowsFlatFiles = false,
        AllowsManagedConnection = false,
        AllowsSourceConnection = true,
        AllowsTargetConnection = false,
        AllowsUserPassword = true,
        AllowsWindowsAuth = false,
        RequiresDatabase = true,
        RequiresLocalStorage = false
    )]
    public class ConnectionDexih : Connection
    {
        public override bool CanBulkLoad => false;
        public override bool CanSort => true;
        public override bool CanFilter => true;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanGroup => false;
	    public override bool CanUseBinary => true;
        public override bool CanUseArray => true;
        public override bool CanUseJson => true;
        public override bool CanUseXml => true;
        public override bool CanUseCharArray => true;
        public override bool CanUseDbAutoIncrement => false;
	    public override bool CanUseSql => false;
        public override bool DynamicTableCreation => false;
        
        private bool _isAuthenticated;
        private DexihActiveAgent _activeAgent;
        private DownloadUrl _downloadUrl;

        public KeyValuePair<string, IEnumerable<string>>[] _authenticationHeaders;
        
        public string ServerUrl()
        {
            var url = Server;
            if (url.Substring(url.Length - 1) != "/") url += "/";
            return url;
        }

        private void ThrowError(JsonDocument document)
        {
            if (document == null)
            {
                throw new ConnectionException($"Unknown error");
            }
            var message = document?.RootElement.GetProperty("message").GetString();
            var exception = document?.RootElement.GetProperty("exceptionDetails").GetString();

            if (string.IsNullOrEmpty(message))
            {
                throw new ConnectionException($"Unknown error");
            }

            throw new ConnectionException($"Error {message}", new Exception(exception));   
        }
        
        private async Task<HttpContent> HttpPostGetContent(string function, HttpContent content, bool skipLogin = false)
		{
		    try
		    {
                if (!_isAuthenticated && !skipLogin)
                {
                    await Login();
                }

		        try
		        {
                    var client = ClientFactory.CreateClient();
		            var url = ServerUrl() + "Reader/" + function;
                    var request = new HttpRequestMessage(HttpMethod.Post, url) {Content = content};
                    foreach (var header in _authenticationHeaders)
                    {
                        request.Headers.Add(header.Key, header.Value);
                    }
                    
                    var response = await client.SendAsync(request);

                    if (response.IsSuccessStatusCode)
                    {
                        return response.Content;
                    }
                    else
                    {
                        var result = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
                        var message = result.RootElement.GetProperty("message").GetString();
                        result.RootElement.TryGetProperty("exceptionDetails", out var exceptionDetails);
                        if (exceptionDetails.ValueKind == JsonValueKind.String)
                        {
                            throw new ConnectionException(message, new Exception(exceptionDetails.GetString()));    
                        }
                        else
                        {
                            throw new ConnectionException(message);    
                        }
                        
                    }
                    
                    throw new ConnectionException($"Could not connect to server {Server}\n. Response: {response.StatusCode.ToString()} - {response.ReasonPhrase}");
                }
		        catch (HttpRequestException ex)
		        {
		            throw new ConnectionException($"Could not connect to server {Server}\n. {ex.Message}", ex);
		        }
		    }
		    catch (ConnectionException)
		    {
		        throw;
		    }
            catch(Exception ex)
            {
                throw new ConnectionException($"Http post failed. {ex.Message}", ex);
            }
        }

		public async Task<JsonDocument> HttpPost(string function, HttpContent content, bool skipLogin = false, CancellationToken cancellationToken = default)
        {
            var responseContent = await HttpPostGetContent(function, content, skipLogin);
            var result = await JsonDocument.ParseAsync(await responseContent.ReadAsStreamAsync(), cancellationToken: cancellationToken);
            return result;
        }

        public async Task<string> HttpPostRaw(string function, HttpContent content, bool skipLogin = false)
        {
            var responseContent = await HttpPostGetContent(function, content, skipLogin);
            var responseString = await responseContent.ReadAsStringAsync();
            return responseString;
        }

        /// <summary>
		/// Logs into the dexih instance and returns the cookiecontainer which can be used to authenticate future requests.
		/// </summary>
		/// <returns>The login.</returns>
		private async Task Login()
		{
            try
            {
                var client = ClientFactory.CreateClient();
                
                //Login to the web server, which will allow future connections to be authenticated.
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("User", Username),
                    new KeyValuePair<string, string>("Password", Password),
                    new KeyValuePair<string, string>("HubName", DefaultDatabase)
                });

                try
                {
                    var uri = new Uri(ServerUrl() + "Reader/Login");
                    var request = new HttpRequestMessage(HttpMethod.Post, uri) {Content = content};
                    var response = await client.SendAsync(request);
                    if (response.IsSuccessStatusCode)
                    {
                        var result = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
                        
                        if (result.RootElement.GetProperty("success").GetBoolean())
                        {
                            if (result.RootElement.TryGetProperty("activeAgent", out var agents))
                            {
                                _activeAgent = agents.ToObject<DexihActiveAgent>();
                            }
                            
                            _authenticationHeaders = response.Headers.Where(c => c.Key == "Set-Cookie").ToArray();
                            _isAuthenticated = true;
                        }
                        else
                        {
                            ThrowError(result);
                        }
                    }
                    else
                    {
                        var result = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync());
                        var message = result.RootElement.GetProperty("message").GetString();
                        result.RootElement.TryGetProperty("exceptionDetails", out var exceptionDetailsElement);
                        var exceptionDetails = exceptionDetailsElement.ValueKind == JsonValueKind.Undefined ? "" : exceptionDetailsElement.GetString();
                        throw new ConnectionException(message, new Exception(exceptionDetails));
                    }
                }
                catch (HttpRequestException ex)
                {
                    throw new ConnectionException($"Could not connect to server {Server}. {ex.Message}", ex);
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Login failed.\n{ex.Message}", ex);
            }
        }
        
        public async Task<string> GetRemoteAgentInstanceId()
        {
            if (!_isAuthenticated)
            {
                await Login();
            }
            
            if (_activeAgent == null)
            {
                throw new ConnectionException($"There is no remote agent available for the hub {DefaultDatabase}.");
            }

            return _activeAgent.InstanceId;
        }

        public async Task<DownloadUrl> GetDownloadUrl()
        {
            if (!_isAuthenticated)
            {
                await Login();
            }

            if (_activeAgent == null)
            {
                throw new ConnectionException($"There is no remote agent available for the hub {DefaultDatabase}.");
            }

            if (_downloadUrl == null)
            {
                if (_activeAgent.DownloadUrls.Length == 0)
                {
                    throw new ConnectionException($"There are no download urls available for the remote agent {_activeAgent.Name}.");
                }

                foreach (var downloadUrl in _activeAgent.DownloadUrls)
                {
                    var client = ClientFactory.CreateClient();
                    var response = await client.GetAsync(downloadUrl.Url + "/ping");
                    if (response.IsSuccessStatusCode)
                    {
                        _downloadUrl = downloadUrl;
                        return downloadUrl;
                    }
                }

                throw new ConnectionException($"Could not connect with any of the download urls for the remote agent {_activeAgent.Name}.");
            }

            return _downloadUrl;
        }

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            try
            {
                var result = await HttpPost("GetHubs", null, cancellationToken: cancellationToken);
                var hubList = result.ToObject<List<string>>();
                return hubList;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Get integration hub hubs failed. {ex.Message}", ex);
            }
        }

		public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
		{
            try
            {
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("HubName", DefaultDatabase)
                });

                var result = await HttpPost("GetTables", content, cancellationToken: cancellationToken);
                var tableList = result.RootElement.ToObject<List<Table>>();
                return tableList;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Get table list failed.\n{ex.Message}", ex);
            }
        }

        /// <inheritdoc />
        /// <summary>
        /// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
        /// </summary>
        /// <param name="importTable"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public override async Task<Table> GetSourceTableInfo(Table importTable, CancellationToken cancellationToken = default)
         {
			try
			{
				var content = new FormUrlEncodedContent(new[]
				{
					new KeyValuePair<string, string>("HubName", DefaultDatabase),
                    new KeyValuePair<string, string>("SourceConnectionName", importTable.SourceConnectionName),
                    new KeyValuePair<string, string>("TableSchema", importTable.Schema),
                    new KeyValuePair<string, string>("TableName", importTable.Name),
				});

				var result = await HttpPost("GetTableInfo", content, cancellationToken: cancellationToken);
                var table = result.RootElement.ToObject<Table>();
                return table;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get source table information for table {importTable.Name} failed.\n{ex.Message}", ex);
            }
        }

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="filters"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public object[] LookupRow(Table table, Filters filters, CancellationToken cancellationToken = default)
        {
			throw new NotImplementedException();
		}

        public override Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<Table> InitializeTable(Table table, int position)
        {
            return Task.FromResult(table);
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> query, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteDelete(Table table, List<DeleteQuery> query, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<long> ExecuteInsert(Table table, List<InsertQuery> query, int transactionReference, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
        {
			throw new NotImplementedException();
		}

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            var reader = new ReaderDexih(this, table);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }

        public override Task<bool> CompareTable(Table table, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(true);
        }

        public override void Dispose()
        {
            // _httpClient?.Dispose();
            base.Dispose();
        }
    }

    
}
