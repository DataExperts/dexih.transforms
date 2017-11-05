using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.transforms;
using dexih.functions;
using System.Data.Common;
using System.Text.RegularExpressions;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Net;
using Dexih.Utils.CopyProperties;
using dexih.transforms.Exceptions;
using dexih.functions.Query;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.DataType;

namespace dexih.connections.webservice
{
    
    public class ConnectionRestful : Connection
    {
        public override string ServerHelp => "The API end point for the Restful web service, excluding query strings.  Eg.  http://twitter.com/statuses/";
        public override string DefaultDatabaseHelp => "Service Name";
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override bool CanBulkLoad => false;
        public override bool CanSort => false;
        public override bool CanFilter => false;
        public override bool CanDelete => false;
        public override bool CanUpdate => false;
        public override bool CanAggregate => false;
        public override bool CanUseBinary => false;
        public override bool CanUseSql => false;
        public override bool DynamicTableCreation => false;


        public override string DatabaseTypeName => "Restful Web Service";
        public override ECategory DatabaseCategory => ECategory.WebService;

        public override Task CreateTable(Table table, bool dropTable, CancellationToken cancellationToken)
        {
			throw new ConnectionException("Create table cannot be used as the webservice is readonly.");
        }

        public override Task<List<string>> GetDatabaseList(CancellationToken cancellationToken)
        {
            return Task.FromResult(new List<string>());
        }

		/// <summary>
		/// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public override async Task<Table> GetSourceTableInfo(Table table, CancellationToken cancellationToken)
        {
            try
            {
				var restFunction = (RestFunction)table;

                if (string.IsNullOrEmpty( restFunction.RestfulUri))
                {
                    throw new ConnectionException($"The restful uri has not been specified.");
                }

                var restfulUri = restFunction.RestfulUri;
                var rowPath = restFunction.RowPath;

				var newRestFunction = new RestFunction
				{
					Name = restFunction.Name,
					Description = "",
					RestfulUri = restfulUri,
					LogicalName = restFunction.Name
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
						Datatype = ETypeCode.String,
						DeltaType = TableColumn.EDeltaType.NaturalKey,
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
                    Datatype = ETypeCode.String,
                    DeltaType = TableColumn.EDeltaType.TrackingField,
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
                    Datatype = ETypeCode.String,
                    DeltaType = TableColumn.EDeltaType.TrackingField,
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
                    Datatype = ETypeCode.Boolean,
                    DeltaType = TableColumn.EDeltaType.TrackingField,
                    MaxLength = null,
                    Description = "Is the web service call successful.",
                    AllowDbNull = true,
                    IsUnique = false
                };
                newRestFunction.Columns.Add(col);

                var query = new SelectQuery();
                query.Columns.Add(new SelectColumn(new TableColumn("Response"), SelectColumn.EAggregate.None));
                query.Columns.Add(new SelectColumn(new TableColumn("ResponseSuccess"), SelectColumn.EAggregate.None));
                query.Table = newRestFunction.Name;
                query.Rows = 1;

                if (newRestFunction.Columns.Count > 0)
                {
                    var data = await GetPreview(newRestFunction, query, cancellationToken);

                    var reader = data.Data;
                    JToken content;

                    try
                    {
                        content = JToken.Parse(reader[0][newRestFunction.GetOrdinal("Response")].ToString());
                    }
                    catch (Exception ex)
                    {
                        throw new ConnectionException($"Failed to parse the response json value. {ex.Message}", ex, reader[0][newRestFunction.GetOrdinal("Response")].ToString());
                    }

                    if (content != null)
                    {
                        if(string.IsNullOrEmpty(rowPath)) {
                            content.Children();
                        } 
                        else 
                        {
                            content.SelectTokens(rowPath);
                        }

                        if(content.Type == JTokenType.Array)
                        {
                            content = content.First();
                        }
                        
                        foreach (var child in content.Children())
                        {

                            if (child.Type == JTokenType.Property)
                            {
                                var value = (JProperty)child;
								col = new TableColumn
								{
									Name = value.Name,
									IsInput = false,
									LogicalName = value.Path,
									Datatype = DataType.GetTypeCode(value.Value.Type),
									DeltaType = TableColumn.EDeltaType.TrackingField,
									MaxLength = null,
									Description = "Json value of the " + value.Path + " path",
									AllowDbNull = true,
									IsUnique = false
								};
								newRestFunction.Columns.Add(col);
							}
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

        public override Task<List<Table>> GetTableList(CancellationToken cancellationToken)
        {
            return Task.FromResult(new List<Table>());
        }


        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="filters"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<object[]> LookupRow(Table table, List<Filter> filters, CancellationToken cancellationToken)
        {
            try
            {
				var restFunction = (RestFunction)table;
                var row = new object[table.Columns.Count];

                var uri = restFunction.RestfulUri;

                foreach (var filter in filters)
                {
                    uri = uri.Replace("{" + filter.Column1.Name + "}", filter.Value2.ToString());
					row[table.GetOrdinal(filter.Column1.TableColumnName())] = filter.Value2.ToString();
                }

                foreach (var column in table.Columns.Where(c => c.IsInput))
                {
                    if(column.DefaultValue != null)
                    {
                        uri = uri.Replace("{" + column.Name + "}", column.DefaultValue);
                    }
                }

                HttpClientHandler handler = null;
                if (!String.IsNullOrEmpty(Username))
                {
                    var credentials = new NetworkCredential(Username, Password);
					var creds = new CredentialCache
					{
						{ new Uri(Server), "basic", credentials },
						{ new Uri(Server), "digest", credentials }
					};
					handler = new HttpClientHandler { Credentials = creds };
                }
                else
                {
                    handler = new HttpClientHandler();
                }

                using (var client = new HttpClient(handler))
                {
                    client.BaseAddress = new Uri(Server);
                    client.DefaultRequestHeaders.Accept.Clear();
                    //client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    var response = await client.GetAsync(uri, cancellationToken);

                    row[table.GetOrdinal("ResponseStatusCode")] = response.StatusCode.ToString();
                    row[table.GetOrdinal("ResponseSuccess")] = response.IsSuccessStatusCode;
                    row[table.GetOrdinal("Response")] = await response.Content.ReadAsStringAsync();

                    if (table.Columns.Count > 3 + filters.Count)
                    {
                        var data = JToken.Parse(row[table.GetOrdinal("Response")].ToString());

                        if(data.Type == JTokenType.Array)
                        {
                            throw new ConnectionException($"Lookup response was an array.");
                        }

                        for (var i = 3 + filters.Count; i < table.Columns.Count; i++)
                        {
                            var value = data.SelectToken(table.Columns[i].Name);

                            try
                            {
                                row[i] = DataType.TryParse(table.Columns[i].Datatype, value);
                            }
                            catch(Exception ex)
                            {
                                throw new ConnectionException($"Failed to convert value on column {table.Columns[i].Name} to datatype {table.Columns[i].Datatype}. {ex.Message}", ex, value);
                            }
                        }
                    }
                }

                return row;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Lookup on the web service {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task TruncateTable(Table table, CancellationToken cancellationToken)
        {
			throw new ConnectionException("Truncate table cannot be used as the webservice is readonly.");
        }

        public override Task<Table> InitializeTable(Table table, int position)
        {
            try
            {
                var restFunction = new RestFunction();
                table.CopyProperties(restFunction, false);
                restFunction.RestfulUri = restFunction.Name;

                return Task.FromResult<Table>(restFunction);
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Initialize table {table.Name} failed. {ex.Message}", ex);

            }
        }

        public override Task ExecuteUpdate(Table table, List<UpdateQuery> queries, CancellationToken cancellationToken)
        {
			throw new ConnectionException("Update table cannot be used as the webservice is readonly.");
        }

        public override Task ExecuteDelete(Table table, List<DeleteQuery> queries, CancellationToken cancellationToken)
        {
			throw new ConnectionException("Delete from table cannot be used as the webservice is readonly.");
        }

        public override Task<long> ExecuteInsert(Table table, List<InsertQuery> queries, CancellationToken cancellationToken)
        {
			throw new ConnectionException("Insert into table cannot be used as the webservice is readonly.");
        }

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancellationToken)
        {
            try
            {
                var lookupResult = await LookupRow(table, query.Filters, cancellationToken);
				var schemaColumn = query.Columns[0].Column.TableColumnName();
                var value = lookupResult[table.GetOrdinal(schemaColumn)];
                return value;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Get value from {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task CreateDatabase(string databaseName, CancellationToken cancellationToken)
        {
			throw new ConnectionException("Create database cannot be used as the webservice is readonly.");
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancellationToken)
        {
			throw new ConnectionException("Webservices do not have database readers.  Use the GetTransformReader function to simulate this.");
        }

        public override Task ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancellationToken)
        {
			throw new ConnectionException("Bulk insert into table cannot be used as the webservice is readonly.");
        }

        public override Transform GetTransformReader(Table table, bool previewMode = false)
        {
            var reader = new ReaderRestful(this, table);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancellationToken)
        {
			return Task.FromResult(true);
        }

    }
}
