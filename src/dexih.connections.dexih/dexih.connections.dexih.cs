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
using System.Diagnostics;
using System.Text;
using dexih.transforms.Exceptions;
using dexih.functions.Query;
using Dexih.Utils.Crypto;

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

        private readonly int _rowsPerBufffer = 1000;
        private string _continuationToken;

        public void SetContinuationToken(string continuationToken)
        {
            _continuationToken = continuationToken; ;
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
                    CookieContainer cookies = new CookieContainer();
                    handler = new HttpClientHandler()
                    {
                        CookieContainer = cookies
                    };
                }

                //Login to the web server to receive an authenicated cookie.
                using (HttpClient httpClient = new HttpClient(handler))
                {
                    HttpResponseMessage response;
                    try
                    {
                        response = await httpClient.PostAsync(Server + "Reader/" + function, content);
                        var responseString = await response.Content.ReadAsStringAsync();
                        JObject result = JObject.Parse(responseString);
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
                CookieContainer cookies = new CookieContainer();
                HttpClientHandler handler = new HttpClientHandler()
                {
                    CookieContainer = cookies
                };

                //Login to the web server to receive an authenicated cookie.
                using (HttpClient httpClient = new HttpClient(handler))
                {
                    var content = new FormUrlEncodedContent(new[]
                    {
                        new KeyValuePair<string, string>("User", Username),
                        new KeyValuePair<string, string>("Password", Password)
                    });

                    HttpResponseMessage response;
                    try
                    {
                        response = await httpClient.PostAsync(Server + "Reader/Login", content);
                    }
                    catch (HttpRequestException ex)
                    {
                        throw new ConnectionException($"Could not connect to server {Server}. {ex.Message}", ex);
                    }

                    var responseString = await response.Content.ReadAsStringAsync();
                    JObject result = JObject.Parse(responseString);
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

        public override Task<bool> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            return Task.FromResult(true);
        }

        public override async Task<List<string>> GetDatabaseList(CancellationToken cancelToken)
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

		public override async Task<List<Table>> GetTableList(CancellationToken cancelToken)
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

        /// <summary>
        /// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <param name="Properties">Mandatory property "RestfulUri".  Additional properties for the default column values.  Use ColumnName=value</param>
        /// <returns></returns>
         public override async Task<Table> GetSourceTableInfo(Table importTable, CancellationToken cancelToken)
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
                throw new ConnectionException($"Get source talbe information for table {importTable.Name} failed. {ex.Message}", ex);
            }
        }

	    /// <summary>
	    /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
	    /// </summary>
	    /// <param name="table"></param>
	    /// <param name="filters"></param>
	    /// <returns></returns>
	    public object[] LookupRow(Table table, List<Filter> filters, CancellationToken cancelToken)
        {
			throw new NotImplementedException();
		}

        public override Task<bool> TruncateTable(Table table, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Table> InitializeTable(Table table, int position)
        {
            return Task.FromResult(table);
        }

        public override Task<long> ExecuteUpdate(Table table, List<UpdateQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<long> ExecuteDelete(Table table, List<DeleteQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<Tuple<long, long>> ExecuteInsert(Table table, List<InsertQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
			throw new NotImplementedException();
		}

        public override Task<bool> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task DataWriterStart(Table table)
        {
			_continuationToken = table.ContinuationToken;
            return Task.CompletedTask;
        }

        public override async Task DataWriterFinish(Table table)
        {
            var content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("ContinuationToken", _continuationToken),
            });

            var result = await HttpPost("PushFinish", content, false);
        }

        public override async Task DataWriterError(string message, Exception exception )
        {
            var content = new FormUrlEncodedContent(new[]
{
                new KeyValuePair<string, string>("ContinuationToken", _continuationToken),
                new KeyValuePair<string, string>("Message", message),
                new KeyValuePair<string, string>("Exception", Json.SerializeObject(exception, ""))
            });

            var result = await HttpPost("SetError", content, false);
        }

        public override async Task<long> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken)
        {
            try
            {
                Stopwatch timer = Stopwatch.StartNew();

                bool readerOpen = true;

                while (readerOpen)
                {
                    var dataSet = new List<object[]>();
                    int bufferCount = 0;

                    while (bufferCount < _rowsPerBufffer && cancellationToken.IsCancellationRequested == false)
                    {
                        readerOpen = await reader.ReadAsync(cancellationToken);

                        if (!readerOpen)
                        {
                            break;
                        }

                        var row = new object[reader.FieldCount];
                        reader.GetValues(row);
                        dataSet.Add(row);
                        bufferCount++;
                    }

                    if (dataSet.Count > 0)
                    {
                        var pushData = new PushData()
                        {
                            ContinuationToken = _continuationToken,
                            IsFinalBuffer = false, // !readerOpen,
                            DataSet = dataSet
                        };

                        string message = Json.SerializeObject(pushData, "");
                        var content = new StringContent(message, Encoding.UTF8, "application/json");

                        var result = await HttpPost("PushData", content, false);

                        if (!(bool)result["success"])
                        {
                            throw new ConnectionException($"Error {result?["message"]}", new Exception(result["exceptionDetails"].ToString()));
                        }

                    }
                }

                return timer.ElapsedTicks;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Insert bulk rows for table {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderDexih(this, table, referenceTransform);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancelToken)
        {
            return Task.FromResult(true);
        }

        public override Task<bool> CompareTable(Table table, CancellationToken cancelToken)
        {
            return Task.FromResult(true);
        }

        private class PushData
        {
            public string ContinuationToken { get; set; }
            public bool IsFinalBuffer { get; set; }
            public List<object[]> DataSet { get; set; }
        }
    }

    
}
