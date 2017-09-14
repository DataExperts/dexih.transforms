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
using Dexih.Utils;
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

        public override Task<bool> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<List<string>> GetDatabaseList(CancellationToken cancelToken)
        {
            return Task.FromResult(new List<string>());
        }

        /// <summary>
        /// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <param name="Properties">Mandatory property "RestfulUri".  Additional properties for the default column values.  Use ColumnName=value</param>
        /// <param name="importTable"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public override async Task<Table> GetSourceTableInfo(Table importTable, CancellationToken cancelToken)
        {
            try
            {
				RestFunction restFunction = (RestFunction)importTable;

                if (string.IsNullOrEmpty( restFunction.RestfulUri))
                {
                    throw new ConnectionException($"The restful uri has not been specified.");
                }

                string restfulUri = restFunction.RestfulUri;
                string rowPath = restFunction.RowPath;

                RestFunction newRestFunction = new RestFunction();
				newRestFunction.Name = restFunction.Name;

                //The new datatable that will contain the table schema
                newRestFunction.Columns.Clear();
                newRestFunction.Description = "";
				newRestFunction.RestfulUri = restfulUri;

                newRestFunction.LogicalName = newRestFunction.Name;

                TableColumn col;
                var inputJoins = new List<JoinPair>();

                //use the regex to extract items in uri between { }.  These will be input columns
                Match match = Regex.Match(restfulUri, @"\{([^}]+)\}");


                while (match.Success)
                {
                    string name = match.Groups[1].Value;

                    col = new TableColumn();

                    //add the basic properties
                    col.Name = name;
                    col.IsInput = true;
                    col.LogicalName = name;
                    col.Datatype = ETypeCode.String;
                    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                    col.MaxLength = 1024;

                    col.Description = "Url Parameter " + name;

                    col.AllowDbNull = true;
                    col.IsUnique = false;

                    //Copy the inputvalue from the table input.  This allows the preview table function below to get sample data.
                    var originalColumn = importTable.Columns.SingleOrDefault(c => c.Name == col.Name);
                    if (originalColumn != null)
                    {
                        var inputValue = originalColumn.DefaultValue;
                        col.DefaultValue = inputValue;
                        inputJoins.Add(new JoinPair()
                        {
                            SourceColumn = col,
                            JoinValue = inputValue
                        });
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

                SelectQuery query = new SelectQuery();
                query.Columns.Add(new SelectColumn(new TableColumn("Response"), SelectColumn.EAggregate.None));
                query.Columns.Add(new SelectColumn(new TableColumn("ResponseSuccess"), SelectColumn.EAggregate.None));
                query.Table = newRestFunction.Name;
                query.Rows = 1;

                if (newRestFunction.Columns.Count > 0)
                {
                    var data = await GetPreview(newRestFunction, query, null, inputJoins, cancelToken);

                    TableCache reader = data.Data;
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
                        IEnumerable<JToken> children;
                        if(string.IsNullOrEmpty(rowPath)) {
                            children = content.Children();
                        } 
                        else 
                        {
                            children = content.SelectTokens(rowPath);
                        }

                        if(content.Type == JTokenType.Array)
                        {
                            content = content.First();
                        }
                        
                        foreach (var child in content.Children())
                        {

                            if (child.Type == JTokenType.Property)
                            {
                                JProperty value = (JProperty)child;
                                col = new TableColumn();
                                col.Name = value.Name;
                                col.IsInput = false;
                                col.LogicalName = value.Path;
                                col.Datatype = DataType.GetTypeCode(value.Value.Type);
                                col.DeltaType = TableColumn.EDeltaType.TrackingField;
                                col.MaxLength = null;
                                col.Description = "Json value of the " + value.Path + " path";
                                col.AllowDbNull = true;
                                col.IsUnique = false;
                                newRestFunction.Columns.Add(col);
                            }
                        }
                    }
                }
                return newRestFunction;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Get web service information for {importTable.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task<List<Table>> GetTableList(CancellationToken cancelToken)
        {
            return Task.FromResult(new List<Table>());
        }


        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="filters"></param>
        /// <returns></returns>
        public async Task<object[]> LookupRow(Table table, List<Filter> filters, CancellationToken cancelToken)
        {
            try
            {
				var restFunction = (RestFunction)table;
                object[] row = new object[table.Columns.Count];

                string uri = restFunction.RestfulUri;

                foreach (var filter in filters)
                {
                    uri = uri.Replace("{" + filter.Column1.Name + "}", filter.Value2.ToString());
                    row[table.GetOrdinal(filter.Column1.SchemaColumnName())] = filter.Value2.ToString();
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
                    var creds = new CredentialCache();
                    creds.Add(new Uri(Server), "basic", credentials);
                    creds.Add(new Uri(Server), "digest", credentials);
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

                    HttpResponseMessage response = await client.GetAsync(uri, cancelToken);

                    row[table.GetOrdinal("ResponseStatusCode")] = response.StatusCode.ToString();
                    row[table.GetOrdinal("ResponseSuccess")] = response.IsSuccessStatusCode;
                    row[table.GetOrdinal("Response")] = await response.Content.ReadAsStringAsync();

                    if (table.Columns.Count > 3 + filters.Count)
                    {
                        JToken data = JToken.Parse(row[table.GetOrdinal("Response")].ToString());

                        if(data.Type == JTokenType.Array)
                        {
                            throw new ConnectionException($"Lookup response was an array.");
                        }

                        for (int i = 3 + filters.Count; i < table.Columns.Count; i++)
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

        public override Task<bool> TruncateTable(Table table, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
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

        public override async Task<object> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            try
            {
                var lookupResult = await LookupRow(table, query.Filters, cancelToken);
                string schemaColumn = query.Columns[0].Column.SchemaColumnName();
                object value = lookupResult[table.GetOrdinal(schemaColumn)];
                return value;
            }
            catch(Exception ex)
            {
                throw new ConnectionException($"Get value from {table.Name} failed. {ex.Message}", ex);
            }
        }

        public override Task<bool> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<DbDataReader> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<long> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderRestful(this, table, referenceTransform);
            return reader;
        }

        public override Task<bool> TableExists(Table table, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }


    }
}
