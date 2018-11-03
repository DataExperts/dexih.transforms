using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.transforms;
using dexih.functions;
using System.Data.Common;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Threading;
using dexih.transforms.Exceptions;
using dexih.functions.Query;

namespace dexih.connections.dexih
{
    [Connection(
        ConnectionCategory = EConnectionCategory.Hub,
        Name = "Information Hub", 
        Description = "A link to shared data in another hub",
        DatabaseDescription = "Hub Name",
        ServerDescription = "Information Hub Url",
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
        public override bool CanSort => false;
        public override bool CanFilter => false;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanAggregate => false;
	    public override bool CanUseBinary => true;
        public override bool CanUseArray => true;
        public override bool CanUseJson => true;
        public override bool CanUseXml => true;
        public override bool CanUseCharArray => true;
        public override bool CanUseAutoIncrement => false;
	    public override bool CanUseSql => false;
        public override bool DynamicTableCreation => false;
        
        private readonly HttpClient _httpClient = new HttpClient();
        private bool _isAuthenticated = false;
        private DexihActiveAgent _activeAgent;
        private DownloadUrl _downloadUrl;

        private string ServerUrl()
        {
            var url = Server;
            if (url.Substring(url.Length - 1) != "/") url += "/";
            return url;
        }

		public async Task<JObject> HttpPost(string function, HttpContent content, bool skipLogin = false)
		{
		    try
		    {
                if (!_isAuthenticated && !skipLogin)
                {
                    await Login();
                }

		        try
		        {
		            var url = ServerUrl() + "Reader/" + function;
		            var response = await _httpClient.PostAsync(url, content);
		            var responseString = await response.Content.ReadAsStringAsync();
		            var result = JObject.Parse(responseString);
		            return result;
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

		/// <summary>
		/// Logs into the dexih instance and returns the cookiecontainer which can be used to authenticate future requests.
		/// </summary>
		/// <returns>The login.</returns>
		private async Task Login()
		{
            try
            {
                //Login to the web server, which will allow future connections to be authenticated.
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("User", Username),
                    new KeyValuePair<string, string>("Password", Password),
                    new KeyValuePair<string, string>("HubName", DefaultDatabase)
                });

                try
                {
                    var result = await HttpPost("Login", content, true);
                    if ((bool)result["success"])
                    {
                        var agents = result["value"];
                        if (agents != null)
                        {
                            _activeAgent = agents.ToObject<DexihActiveAgent>();
                        }

                        _isAuthenticated = true;
                    }
                    else
                    {
                        throw new ConnectionException($"Error {result?["message"]}", new Exception(result["exceptionDetails"].ToString()));
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
                    throw new ConnectionException($"There are no download urls available for the remoate agent {_activeAgent.Name}.");
                }

                foreach (var downloadUrl in _activeAgent.DownloadUrls)
                {
                    var response = await _httpClient.GetAsync(downloadUrl.Url + "/ping");
                    if (response.IsSuccessStatusCode)
                    {
                        _downloadUrl = downloadUrl;
                        return downloadUrl;
                    }
                }

                throw new ConnectionException($"Could not connect with any of the download urls for the remoate agent {_activeAgent.Name}.");
            }

            return _downloadUrl;
        }

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken)
        {
            try
            {
                var result = await HttpPost("GetHubs", null);
                if ((bool)result["success"])
                {
                    var hubs = result["value"];
                    var hubList = hubs.ToObject<List<string>>();
                    return hubList;
                }
                else
                {
                    throw new ConnectionException($"Error {result?["message"]}", new Exception(result["exceptionDetails"].ToString()));
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Get integration hub hubs failed. {ex.Message}", ex);
            }
        }

		public override async Task<List<Table>> GetTableList(CancellationToken cancellationToken)
		{
            try
            {
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("HubName", DefaultDatabase)
                });

                var result = await HttpPost("GetTables", content);
                if ((bool)result["success"])
                {
                    var tables = result["value"];
                    var tableList = tables.ToObject<List<Table>>();
                    return tableList;
                }
                else
                {
                    throw new ConnectionException($"Error {result?["message"]}");
                }
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
        public override async Task<Table> GetSourceTableInfo(Table importTable, CancellationToken cancellationToken)
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

				var result = await HttpPost("GetTableInfo", content);
                if ((bool)result["success"])
                {
                    var table = result["value"].ToObject<Table>();
                    return table;
                }
                else
                {
                    throw new ConnectionException($"Error {result?["message"]}", new Exception(result["exceptionDetails"].ToString()));
                }
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
        public object[] LookupRow(Table table, List<Filter> filters, CancellationToken cancellationToken)
        {
			throw new NotImplementedException();
		}

        public override Task TruncateTable(Table table, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Table> InitializeTable(Table table, int position)
        {
            return Task.FromResult(table);
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteDelete(Table table, List<DeleteQuery> query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<long> ExecuteInsert(Table table, List<InsertQuery> query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken)
        {
			throw new NotImplementedException();
		}

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Task ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            var reader = new ReaderDexih(this, table);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }

        public override Task<bool> CompareTable(Table table, CancellationToken cancellationToken)
        {
            return Task.FromResult(true);
        }


    }

    
}
