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
        public override bool CanAggregate => false;
	    public override bool CanUseBinary => true;
	    public override bool CanUseSql => false;

        public override string DatabaseTypeName => "Dexih Hub";
        public override ECategory DatabaseCategory => ECategory.Hub;

        private readonly int _rowsPerBufffer = 1000;
        private string _continuationToken;

        public void SetContinuationToken(string continuationToken)
        {
            _continuationToken = continuationToken; ;
        }

		public async Task<ReturnValue<JObject>> HttpPost(string function, HttpContent content, bool authenticate)
		{
            HttpClientHandler handler;
            if (authenticate)
            {
                var loginResult = await Login();
                if (!loginResult.Success)
                {
                    return new ReturnValue<JObject>(loginResult);
                }

                handler = new HttpClientHandler()
                {
                    CookieContainer = loginResult.Value
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
					return new ReturnValue<JObject>(true, result);
				}
				catch (HttpRequestException ex)
				{
					return new ReturnValue<JObject>(false, $"Could not connect to the server at location: {Server}, with the message: {ex.Message}", ex);
				}
				catch (Exception ex)
				{
					return new ReturnValue<JObject>(false, $"Internal error connecting to the server at location:: {Server}, with the message: {ex.Message}", ex);
				}
			}
		}

		/// <summary>
		/// Logs into the dexih instance and returns the cookiecontainer which can be used to authenticate future requests.
		/// </summary>
		/// <returns>The login.</returns>
		private async Task<ReturnValue<CookieContainer>> Login()
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
					return new ReturnValue<CookieContainer>(false, $"Could not connect to the server at location: {Server}, with the message: {ex.Message}", ex);
				}
				catch (Exception ex)
				{
					return new ReturnValue<CookieContainer>(false, $"Internal error connecting to the server at location:: {Server}, with the message: {ex.Message}", ex);
				}

				var responseString = await response.Content.ReadAsStringAsync();
				JObject result = JObject.Parse(responseString);
				if ((bool)result["success"])
				{
					return new ReturnValue<CookieContainer>(true, handler.CookieContainer);
				}
				else
				{
					return new ReturnValue<CookieContainer>(false, $"User authentication failed with message: {result?["message"].ToString()}.", null);
				}
			}
		}

        public override async Task<ReturnValue> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue(true), cancelToken);
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList(CancellationToken cancelToken)
        {
			var result = await HttpPost("GetHubs", null, true);
			if(!result.Success)
			{
				return new ReturnValue<List<string>>(result);
			}

			var hubs = result.Value["hubs"];
			var hubList = hubs.ToObject<List<string>>();

			return new ReturnValue<List<string>>(true, hubList);
        }

		public override async Task<ReturnValue<List<Table>>> GetTableList(CancellationToken cancelToken)
		{
			var content = new FormUrlEncodedContent(new[]
			{
				new KeyValuePair<string, string>("HubName", DefaultDatabase)
			});

			var result = await HttpPost("GetTables", content, true);
			if (!result.Success)
			{
				return new ReturnValue<List<Table>>(result);
			}

			var tables = result.Value["tables"];
			var tableList = tables.ToObject<List<Table>>();

			return new ReturnValue<List<Table>>(true, tableList);
		}

        /// <summary>
        /// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <param name="Properties">Mandatory property "RestfulUri".  Additional properties for the default column values.  Use ColumnName=value</param>
        /// <returns></returns>
         public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table importTable, CancellationToken cancelToken)
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
				if (!result.Success)
				{
					return new ReturnValue<Table>(result);
				}

				if ((bool)result.Value["success"] == false)
				{
					return new ReturnValue<Table>(false, result.Value["message"].ToString(), null);
				}

				var table = result.Value["table"].ToObject<Table>();
                return new ReturnValue<Table>(true, table);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "Error getting table information: " + ex.Message, ex);
            }
        }

	    /// <summary>
	    /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
	    /// </summary>
	    /// <param name="table"></param>
	    /// <param name="filters"></param>
	    /// <returns></returns>
	    public ReturnValue<object[]> LookupRow(Table table, List<Filter> filters, CancellationToken cancelToken)
        {
			throw new NotImplementedException();
		}

        public override async Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue(true), cancelToken);
        }

        public override async Task<ReturnValue<Table>> InitializeTable(Table table, int position)
        {
            return await Task.Run(() => new ReturnValue<Table>(true, table));
        }

        public override Task<ReturnValue<long>> ExecuteUpdate(Table table, List<UpdateQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<long>> ExecuteDelete(Table table, List<DeleteQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<Tuple<long, long>>> ExecuteInsert(Table table, List<InsertQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
			throw new NotImplementedException();
		}

        public override Task<ReturnValue> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue> DataWriterStart(Table table)
        {
            return await Task.Run(() =>
            {
				_continuationToken = table.ContinuationToken;
                // _rowsPerBufffer = table.GetExtendedProperty("rowsPerBuffer");

                return new ReturnValue(true);
            });
        }

        public override async Task<ReturnValue> DataWriterFinish(Table table)
        {
            var content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("ContinuationToken", _continuationToken),
            });

            var result = await HttpPost("PushFinish", content, false);

            return new ReturnValue(true);
        }

        public override async Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader reader, CancellationToken cancellationToken)
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

                        var postResult = await HttpPost("PushData", content, false);
                        if (!postResult.Success)
                        {
                            return new ReturnValue<long>(postResult);
                        }

                        if (postResult.Value["success"].ToString() == "false")
                        {
                            return new ReturnValue<long>(false, postResult.Value["message"].ToString(), new Exception(postResult.Value["exceptionDetails"].ToString()));
                        }
                    }
                }

                return new ReturnValue<long>(true, timer.ElapsedTicks);
            }
            catch (Exception ex)
            {
                return new ReturnValue<long>(false, "The file could not be written to due to the following error: " + ex.Message, ex);
            }
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null )
        {
            var reader = new ReaderDexih(this, table, referenceTransform);
            return reader;
        }

        public override Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue> CompareTable(Table table, CancellationToken cancelToken)
        {
            return await Task.Run(() => new ReturnValue(true), cancelToken);
        }

        private class PushData
        {
            public string ContinuationToken { get; set; }
            public bool IsFinalBuffer { get; set; }
            public List<object[]> DataSet { get; set; }
        }
    }

    
}
