using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.functions.Query;
using dexih.transforms;
using dexih.transforms.Exceptions;
using dexih.transforms.File;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;

namespace dexih.connections.webservice.restful
{
	[Connection(
		ConnectionCategory = EConnectionCategory.WebService,
		Name = "RESTFul Web Service", 
		Description = "RESTful Web Services is a popular specification for web services.  These are only supported for readonly operations.  For bearer authentication set user=\"bearer\" and password to the token value.",
		DatabaseDescription = "",
		ServerDescription = "Restful Service Url (use {0},{1} for parameters)",
		AllowsConnectionString = false,
		AllowsSql = false,
		AllowsFlatFiles = true,
		AllowsManagedConnection = false,
		AllowsSourceConnection = true,
		AllowsTargetConnection = false,
		AllowsUserPassword = true,
		AllowsWindowsAuth = false,
		RequiresDatabase = false,
		RequiresLocalStorage = false
	)]
    public class ConnectionRestful : Connection
    {
        public override bool CanBulkLoad => false;
        public override bool CanSort => false;
        public override bool CanFilter => false;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanGroup => false;
        public override bool CanUseBinary => false;
	    public override bool CanUseArray => false;
	    public override bool CanUseJson => true;
        public override bool CanUseXml => false;
        public override bool CanUseCharArray => false;
        public override bool CanUseSql => false;
	    public override bool CanUseDbAutoIncrement => false;
        public override bool DynamicTableCreation => false;

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken = default)
        {
			throw new ConnectionException("Create table cannot be used as the webservice is readonly.");
        }

        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new List<string>());
        }

		/// <summary>
		/// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public override async Task<Table> GetSourceTableInfo(Table table, CancellationToken cancellationToken = default)
        {
            try
            {
				var restFunction = (WebService)table;

                if (string.IsNullOrEmpty( restFunction.RestfulUri))
                {
                    throw new ConnectionException($"The restful uri has not been specified.");
                }

                var restfulUri = restFunction.RestfulUri;
                var rowPath = restFunction.RowPath;

                var newRestFunction = new WebService
				{
					Name = restFunction.Name,
					Description = "",
					RestfulUri = restfulUri,
					LogicalName = restFunction.Name,
                    FormatType = restFunction.FormatType
                };

				//The new datatable that will contain the table schema
				newRestFunction.Columns.Clear();

                TableColumn col;

                //use the regex to extract items in uri between { }.  These will be input columns
                var match = Regex.Match(restfulUri, @"\{([^}]+)\}");


                while (match.Success)
                {
                    var name = match.Groups[1].Value;

					col = new TableColumn
					{

						//add the basic properties
						Name = name,
						IsInput = true,
						LogicalName = name,
						DataType = ETypeCode.String,
						DeltaType = EDeltaType.NaturalKey,
						MaxLength = 1024,

						Description = "Url Parameter " + name,

						AllowDbNull = true,
						IsUnique = false
					};

					//Copy the inputvalue from the table input.  This allows the preview table function below to get sample data.
					var originalColumn = table.Columns.SingleOrDefault(c => c.Name == col.Name);
                    if (originalColumn != null)
                    {
                        var inputValue = originalColumn.DefaultValue;
                        col.DefaultValue = inputValue;
                    }

                    newRestFunction.Columns.Add(col);
                    match = match.NextMatch();
                }


                //This column is use to capture the entire response from the web services call.
                col = new TableColumn()
                {
                    Name = "Response",
                    IsInput = false,
                    LogicalName = "Response",
                    DataType = newRestFunction.FormatType,
                    DeltaType = EDeltaType.ResponseData,
                    MaxLength = null,
                    Description = "Response content from the service",
                    AllowDbNull = true,
                    IsUnique = false
                };
                newRestFunction.Columns.Add(col);

                col = new TableColumn()
                {
                    Name = "ResponseStatusCode",
                    IsInput = false,
                    LogicalName = "ResponseStatusCode",
                    DataType = ETypeCode.String,
                    DeltaType = EDeltaType.ResponseStatus,
                    MaxLength = null,
                    Description = "The status code returned by the service",
                    AllowDbNull = true,
                    IsUnique = false
                };
                newRestFunction.Columns.Add(col);

                col = new TableColumn()
                {
                    Name = "ResponseSuccess",
                    IsInput = false,
                    LogicalName = "ResponseSuccess",
                    DataType = ETypeCode.Boolean,
                    DeltaType = EDeltaType.ResponseSuccess,
                    MaxLength = null,
                    Description = "Is the web service call successful.",
                    AllowDbNull = true,
                    IsUnique = false
                };
                newRestFunction.Columns.Add(col);

				col = new TableColumn()
				{
					Name = "ResponseError",
					IsInput = false,
					LogicalName = "ResponseError",
					DataType = ETypeCode.String,
					DeltaType = EDeltaType.Error,
					MaxLength = null,
					Description = "Error message calling the web service.",
					AllowDbNull = true,
					IsUnique = false
				};
				newRestFunction.Columns.Add(col);

				col = new TableColumn()
				{
					Name = "Url",
					IsInput = false,
					LogicalName = "Url",
					DataType = ETypeCode.String,
					DeltaType = EDeltaType.Url,
					MaxLength = null,
					Description = "Url used to call the web service.",
					AllowDbNull = true,
					IsUnique = false
				};
				newRestFunction.Columns.Add(col);

                var query = new SelectQuery();
                query.Columns.Add(new SelectColumn(new TableColumn("Response")));
                query.Columns.Add(new SelectColumn(new TableColumn("ResponseSuccess")));
                query.TableName = newRestFunction.Name;
                query.Rows = 1;

                if (newRestFunction.Columns.Count > 0)
                {
	                var response = await GetWebServiceResponse(newRestFunction, query?.Filters, cancellationToken);

	                if (!response.isSuccess)
	                {
		                throw new ConnectionException($"The web service called failed with response {response.statusCode}.  Url used: {response.url}");
	                }

	                var dataStream = response.response;


					ICollection<TableColumn> fileColumns = null;

					switch (newRestFunction.FormatType)
					{
						case ETypeCode.Json:
							var jsonHandler = new FileHandlerJson(restFunction, rowPath);
							fileColumns = await jsonHandler.GetSourceColumns(dataStream);
							break;
						case ETypeCode.Xml:
							var xmlHandler = new FileHandlerXml(restFunction, rowPath);
							fileColumns = await xmlHandler.GetSourceColumns(dataStream);
							break;
						case ETypeCode.Text:
							var textHandler = new FileHandlerText(restFunction, restFunction.FileConfiguration);
							fileColumns = await textHandler.GetSourceColumns(dataStream);
							break;
					}

					if (fileColumns != null)
					{
						foreach (var column in fileColumns)
						{
							newRestFunction.Columns.Add(column);
						}
					}
                }

                return newRestFunction;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get web service information for {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task<List<Table>> GetTableList(CancellationToken cancellationToken = default)
        {
            return Task.FromResult(new List<Table>());
        }

        private async Task<(string url, string statusCode, bool isSuccess, Stream response)> GetWebServiceResponse(WebService restFunction, Filters filters, CancellationToken cancellationToken = default)
        {
            var uri = restFunction.RestfulUri;

            foreach (var filter in filters)
            {
                uri = uri.Replace("{" + filter.Column1.Name + "}", filter.Value2?.ToString()) ?? "";
            }

            foreach (var column in restFunction.Columns.Where(c => c.IsInput))
            {
                if (column.DefaultValue != null)
                {
                    uri = uri.Replace("{" + column.Name + "}", column.DefaultValue.ToString());
                }
            }
            
            HttpClientHandler handler = null;
            if (!string.IsNullOrEmpty(Username))
            {
                var credentials = new NetworkCredential(Username, Password);
                var creds = new CredentialCache
                    {
                        { new Uri(Server), "basic", credentials },
                        { new Uri(Server), "digest", credentials },
                    };
                handler = new HttpClientHandler { Credentials = creds };
            }
            else
            {
                handler = new HttpClientHandler();
            }
            


			Uri completeUri;

			if (!Server.EndsWith("/") && !Server.Contains("?") && !uri.StartsWith("/"))
			{
				completeUri = new Uri(Server + "/" + uri);
			}
			else
			{
				completeUri = new Uri(Server + uri);
			}

            using (var client = new HttpClient(handler))
            {
				client.BaseAddress = new Uri(completeUri.GetLeftPart(UriPartial.Authority));
                client.DefaultRequestHeaders.Accept.Clear();

                if (string.Compare(Username, "bearer", StringComparison.OrdinalIgnoreCase) == 0)
                {
	                client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", Password);
                }

                var response = await client.GetAsync(completeUri.PathAndQuery, cancellationToken);

				return (completeUri.ToString(), response.StatusCode.ToString(), response.IsSuccessStatusCode, await response.Content.ReadAsStreamAsync());
            }
        }

      /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="filters"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<ICollection<object[]>> LookupRow(Table table, Filters filters, CancellationToken cancellationToken = default)
        {
            try
            {
                var restFunction = (WebService)table;

                var response = await GetWebServiceResponse(restFunction, filters, cancellationToken);

                var responseStatusOrdinal = restFunction.GetOrdinal(EDeltaType.ResponseStatus);
                var responseSuccessOrdinal = restFunction.GetOrdinal(EDeltaType.ResponseSuccess);
                var responseDataOrdinal = restFunction.GetOrdinal(EDeltaType.ResponseData);
				var urlOrdinal = restFunction.GetOrdinal(EDeltaType.Url);
				var errorOrdinal = restFunction.GetOrdinal(EDeltaType.Error);

                var lookupResult = new List<object[]>();

                void PopulateRow(object[] baseRow)
                {
                    if (responseStatusOrdinal >= 0)
                    {
                        baseRow[responseStatusOrdinal] = response.statusCode;
                    }

                    if (responseSuccessOrdinal >= 0)
                    {
                        baseRow[responseSuccessOrdinal] = response.isSuccess;
                    }

                    if (urlOrdinal >= 0)
                    {
                        baseRow[urlOrdinal] = response.url;
                    }

                    foreach (var column in restFunction.Columns.Where(c => c.IsInput))
                    {
                        if (filters != null)
                        {
                            var filter = filters.Where(c => c.Column1.Name == column.Name).ToArray();
                            if (!filter.Any())
                            {
                                baseRow[restFunction.GetOrdinal(column)] = column.DefaultValue;
                            }
                            else
                            {
                                baseRow[restFunction.GetOrdinal(column)] = filter.First().Value2;
                            }
                        }
                        else
                        {
                            baseRow[restFunction.GetOrdinal(column)] = column.DefaultValue;
                        }
                    }
                }

				if (response.isSuccess)
				{
					FileHandlerBase fileHandler = null;

					switch (restFunction.FormatType)
					{
						case ETypeCode.Text:
							fileHandler = new FileHandlerText(restFunction, restFunction.FileConfiguration);
							break;
						case ETypeCode.Json:
							fileHandler = new FileHandlerJson(restFunction, restFunction.RowPath);
							break;
						case ETypeCode.Xml:
							fileHandler = new FileHandlerXml(restFunction, restFunction.RowPath);
							break;
						default:
							throw new ArgumentOutOfRangeException();
					}

					await fileHandler.SetStream(response.response, null);
					var rows = await fileHandler.GetAllRows();
                    foreach(var row in rows)
                    {
                        PopulateRow(row);
                    }
                    return rows;
				} 
				else
				{
                    var row = new object[table.Columns.Count];
                    PopulateRow(row);

					if(errorOrdinal >= 0)
					{
						var reader = new StreamReader(response.response);
						var errorString = await reader.ReadToEndAsync();
                        row[errorOrdinal] = errorString;
					}

					return new[] { row };
				}
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Lookup on the web service {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task TruncateTable(Table table, int transactionReference, CancellationToken cancellationToken = default)
        {
			throw new ConnectionException("Truncate table cannot be used as the webservice is readonly.");
        }

        public override Task<Table> InitializeTable(Table table, int position)
        {
            try
            {
                var restFunction = new WebService();
                table.CopyProperties(restFunction, false);
                restFunction.RestfulUri = restFunction.Name;

                return Task.FromResult<Table>(restFunction);
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Initialize table {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
			throw new ConnectionException("Update table cannot be used as the webservice is readonly.");
        }

        public override Task ExecuteDelete(Table table, List<DeleteQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
			throw new ConnectionException("Delete from table cannot be used as the webservice is readonly.");
        }

        public override Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, int transactionReference, CancellationToken cancellationToken = default)
        {
			throw new ConnectionException("Insert into table cannot be used as the webservice is readonly.");
        }

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken = default)
        {
            try
            {
                var lookupResult = await LookupRow(table, query.Filters, cancellationToken);
                if(lookupResult == null || !lookupResult.Any())
                {
                    return null;
                }
				var schemaColumn = query.Columns[0].Column.TableColumnName();
                var value = lookupResult.First()[table.GetOrdinal(schemaColumn)];
                return value;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Get value from {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken = default)
        {
			throw new ConnectionException("Create database cannot be used as the webservice is readonly.");
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken = default)
        {
			throw new ConnectionException("Webservices do not have database readers.  Use the GetTransformReader function to simulate this.");
        }

        public override Task ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancellationToken = default)
        {
			throw new ConnectionException("Bulk insert into table cannot be used as the webservice is readonly.");
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            var reader = new ReaderRestful(this, table);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken = default)
        {
			return Task.FromResult(true);
        }

    }
}
