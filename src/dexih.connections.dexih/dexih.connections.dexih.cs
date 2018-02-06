using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.transforms;
using dexih.functions;
using System.Data.Common;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Net;
using dexih.transforms.Exceptions;
using dexih.functions.Query;

namespace dexih.connections.dexih
{
    
    public class ConnectionDexih : Connection
    {
        public override string ServerHelp => "The URI for the Integration Hub";
        public override string DefaultDatabaseHelp => "Hub Name";
        public override bool AllowNtAuth => false;
        public override bool AllowUserPass => true;
        public override bool CanBulkLoad => false;
        public override bool CanSort => false;
        public override bool CanFilter => false;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanAggregate => false;
	    public override bool CanUseBinary => true;
	    public override bool CanUseSql => false;
        public override bool DynamicTableCreation => false;


        public override string DatabaseTypeName => "Dexih Hub";
        public override ECategory DatabaseCategory => ECategory.Hub;

        private string ServerUrl()
        {
            var url = Server;
            if (url.Substring(url.Length - 1) != "/") url += "/";
            return url;
        }

		public async Task<JObject> HttpPost(string function, HttpContent content, bool authenticate)
		{
            try
            {
                HttpClientHandler handler;
                if (authenticate)
                {
                    var loginCookie = await Login();

                    handler = new HttpClientHandler()
                    {
                        CookieContainer = loginCookie
                    };
                }
                else
                {
                    var cookies = new CookieContainer();
                    handler = new HttpClientHandler()
                    {
                        CookieContainer = cookies
                    };
                }

                //Login to the web server to receive an authenicated cookie.
                using (var httpClient = new HttpClient(handler))
                {
                    try
                    {
                        var response = await httpClient.PostAsync(ServerUrl() + "Reader/" + function, content);
                        var responseString = await response.Content.ReadAsStringAsync();
                        var result = JObject.Parse(responseString);
                        return result;
                    }
                    catch (HttpRequestException ex)
                    {
                        throw new ConnectionException($"Could not connect to server {Server}. {ex.Message}", ex);
                    }
                }
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
		private async Task<CookieContainer> Login()
		{
            try
            {
                var cookies = new CookieContainer();
                var handler = new HttpClientHandler()
                {
                    CookieContainer = cookies
                };

                //Login to the web server to receive an authenicated cookie.
                using (var httpClient = new HttpClient(handler))
                {
                    var content = new FormUrlEncodedContent(new[]
                    {
                        new KeyValuePair<string, string>("User", Username),
                        new KeyValuePair<string, string>("Password", Password)
                    });

                    HttpResponseMessage response;
                    try
                    {
                        response = await httpClient.PostAsync(ServerUrl() + "Reader/Login", content);
                    }
                    catch (HttpRequestException ex)
                    {
                        throw new ConnectionException($"Could not connect to server {Server}. {ex.Message}", ex);
                    }

                    var responseString = await response.Content.ReadAsStringAsync();
                    var result = JObject.Parse(responseString);
                    if ((bool)result["success"])
                    {
                        return handler.CookieContainer;
                    }
                    else
                    {
                        throw new ConnectionException($"User authentication error {result?["message"]}", new Exception(result["exceptionDetails"].ToString()));
                    }
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Login failed. {ex.Message}", ex);
            }
        }

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override async Task<List<string>> GetDatabaseList(CancellationToken cancellationToken)
        {
            try
            {
                var result = await HttpPost("GetHubs", null, true);
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

                var result = await HttpPost("GetTables", content, true);
                if ((bool)result["success"])
                {
                    var tables = result["value"];
                    var tableList = tables.ToObject<List<Table>>();
                    return tableList; ;
                }
                else
                {
                    throw new ConnectionException($"Error {result?["message"]}");
                }
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Get table list failed. {ex.Message}", ex);
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

				var result = await HttpPost("GetTableInfo", content, true);
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
                throw new ConnectionException($"Get source table information for table {importTable.Name} failed. {ex.Message}", ex);
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
