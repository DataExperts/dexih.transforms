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
using System.IO;
using System.Xml.XPath;

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

        /// <summary>
        /// Add a "/" to the server name if it is not already there.
        /// </summary>
        public override string Server {
            get => base.Server;
            set
            {
                if(!value.EndsWith("/"))
                {
                    base.Server = value + "/";
                }
                else
                {
                    base.Server = value;
                }
            }
        }

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
                    Datatype = newRestFunction.FormatType,
                    DeltaType = TableColumn.EDeltaType.ResponseData,
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
                    DeltaType = TableColumn.EDeltaType.ResponseStatus,
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
                    DeltaType = TableColumn.EDeltaType.ResponseSuccess,
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

                    if (newRestFunction.FormatType == ETypeCode.Json)
                    {

                        JToken content;
                        try
                        {
                            content = JToken.Parse(reader[0][newRestFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData)].ToString());
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Failed to parse the response json value. {ex.Message}", ex, reader[0][newRestFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData)].ToString());
                        }

                        if (content != null)
                        {
                            IEnumerable<JToken> tokens;
                            if (string.IsNullOrEmpty(rowPath))
                            {
                                if (content.Type == JTokenType.Array)
                                {
                                    tokens = content.First().Children();
                                }
                                else
                                {
                                    tokens = content.Children();
                                }
                            }
                            else
                            {
                                tokens = content.SelectTokens(rowPath).First().Children();
                            }

                            foreach (var child in tokens)
                            {

                                if (child.Type == JTokenType.Property)
                                {
                                    var value = (JProperty)child;
                                    ETypeCode dataType;
                                    if (value.Value.Type == JTokenType.Array || value.Value.Type == JTokenType.Object || value.Value.Type == JTokenType.Property)
                                    {
                                        dataType = ETypeCode.Json;
                                    }
                                    else
                                    {
                                        dataType = DataType.GetTypeCode(value.Value.Type);
                                    }
                                    col = new TableColumn
                                    {
                                        Name = value.Name,
                                        IsInput = false,
                                        LogicalName = value.Name,
                                        Datatype = dataType,
                                        DeltaType = TableColumn.EDeltaType.ResponseSegment,
                                        MaxLength = null,
                                        Description = "Json value of the " + value.Path + " path",
                                        AllowDbNull = true,
                                        IsUnique = false
                                    };
                                    newRestFunction.Columns.Add(col);
                                }
                                else
                                {
                                    col = new TableColumn
                                    {
                                        Name = child.Path,
                                        IsInput = false,
                                        LogicalName = child.Path,
                                        Datatype = ETypeCode.Json,
                                        DeltaType = TableColumn.EDeltaType.ResponseSegment,
                                        MaxLength = null,
                                        Description = "Json from the " + child.Path + " path",
                                        AllowDbNull = true,
                                        IsUnique = false
                                    };
                                    newRestFunction.Columns.Add(col);
                                }
                            }
                        }
                    }


                    if (newRestFunction.FormatType == ETypeCode.Xml)
                    {

                        XPathNavigator xPathNavigator;

                        try
                        {
                            var stream = new StringReader(reader[0][newRestFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData)].ToString());
                            var xPathDocument = new XPathDocument(stream);
                            xPathNavigator = xPathDocument.CreateNavigator();
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException($"Failed to parse the response xml value. {ex.Message}", ex, reader[0][newRestFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData)].ToString());
                        }

                        if (xPathNavigator != null)
                        {
                            XPathNodeIterator nodes;
                            if (string.IsNullOrEmpty(rowPath))
                            {
                                nodes = xPathNavigator.SelectChildren(XPathNodeType.All);
                                if(nodes.Count == 1)
                                {
                                    nodes.MoveNext();
                                    nodes = nodes.Current.SelectChildren(XPathNodeType.All);
                                }
                            }
                            else
                            {
                                nodes = xPathNavigator.Select(rowPath);
                                if(nodes.Count < 0)
                                {
                                    throw new ConnectionException($"Failed to find the path {rowPath} in the xml response.");
                                }

                                nodes.MoveNext();
                                nodes = nodes.Current.SelectChildren(XPathNodeType.All);
                            }

                            Dictionary<string, int> columnCounts = new Dictionary<string, int>();
                            
                            while(nodes.MoveNext())
                            {
                                var node = nodes.Current;

                                string nodePath;
                                if(columnCounts.ContainsKey(node.Name))
                                {
                                    var count = columnCounts[node.Name];
                                    count++;
                                    columnCounts[node.Name] = count;
                                    nodePath = $"{node.Name}[{count}]";
                                }
                                else
                                {
                                    columnCounts.Add(node.Name, 1);
                                    nodePath = $"{node.Name}[1]";
                                }

                                if (node.SelectChildren(XPathNodeType.All).Count == 1)
                                {
                                    var dataType = DataType.GetTypeCode(node.ValueType);
                                    col = new TableColumn
                                    {
                                        Name = nodePath,
                                        IsInput = false,
                                        LogicalName = node.Name,
                                        Datatype = dataType,
                                        DeltaType = TableColumn.EDeltaType.ResponseSegment,
                                        MaxLength = null,
                                        Description = "Value of the " + nodePath + " path",
                                        AllowDbNull = true,
                                        IsUnique = false
                                    };
                                    newRestFunction.Columns.Add(col);
                                }
                                else
                                {
                                    col = new TableColumn
                                    {
                                        Name = nodePath,
                                        IsInput = false,
                                        LogicalName = node.Name,
                                        Datatype = ETypeCode.Xml,
                                        DeltaType = TableColumn.EDeltaType.ResponseSegment,
                                        MaxLength = null,
                                        Description = "Xml from the " + nodePath + " path",
                                        AllowDbNull = true,
                                        IsUnique = false
                                    };
                                    newRestFunction.Columns.Add(col);
                                }
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

        public async Task<(string statusCode, bool isSuccess, string response)> GetWebServiceResponse(RestFunction restFunction, List<Filter> filters, CancellationToken cancellationToken)
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

                var response = await client.GetAsync(uri, cancellationToken);

                return (response.StatusCode.ToString(), response.IsSuccessStatusCode, await response.Content.ReadAsStringAsync());
            }
        }

        public IEnumerable<object[]> ProcessJson(RestFunction restFunction, object[] baseRow, string data)
        {
            var content = JToken.Parse(data);
            IEnumerator<JToken> jsonIterator;

            if (string.IsNullOrEmpty(restFunction.RowPath))
            {
                jsonIterator = content.AsJEnumerable().GetEnumerator();
            }
            else
            {
                jsonIterator = content.SelectTokens(restFunction.RowPath).AsJEnumerable().GetEnumerator();
            }

            var rows = new List<object[]>();
            var responseDataOrdinal = restFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData);

            var columnCount = restFunction.Columns.Count;
            while (jsonIterator.MoveNext())
            {
                var currentRow = jsonIterator.Current;
                var row = new object[columnCount];
                Array.Copy(baseRow, row, columnCount);

                if (currentRow != null)
                {
                    if (responseDataOrdinal >= 0)
                    {
                        row[responseDataOrdinal] = currentRow?.ToString() ?? "";
                    }

                    foreach (var column in restFunction.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.ResponseSegment))
                    {
                        object value = currentRow.SelectToken(column.Name);
                        try
                        {
                            row[restFunction.GetOrdinal(column)] = DataType.TryParse(column.Datatype, value);
                        }
                        catch (Exception ex)
                        {
                            throw new ConnectionException(
                                $"Failed to convert value on column {column.Name} to datatype {column.Datatype}. {ex.Message}",
                                ex, value);
                        }
                    }
                }

                rows.Add(row);
            }

            return rows;
        }

        public IEnumerable<object[]> ProcessXml(RestFunction restFunction, object[] baseRow, string data)
        {
            var stream = new StringReader(data);
            var xPathDocument = new XPathDocument(stream);
            var xPathNavigator = xPathDocument.CreateNavigator();

            XPathNodeIterator iterator;

            if (string.IsNullOrEmpty(restFunction.RowPath))
            {
                iterator = xPathNavigator.SelectChildren(XPathNodeType.All);
            }
            else
            {
                iterator = xPathNavigator.Select(restFunction.RowPath);
            }

            var rows = new List<object[]>();
            var columnCount = restFunction.Columns.Count;
            var responseDataOrdinal = restFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData);

            while (iterator.MoveNext())
            {
                var currentRow = iterator.Current;

                var row = new object[columnCount];
                Array.Copy(baseRow, row, columnCount);

                if (responseDataOrdinal >= 0)
                {
                    row[responseDataOrdinal] = currentRow.OuterXml;
                }

                foreach (var column in restFunction.Columns.Where(c => c.DeltaType == TableColumn.EDeltaType.ResponseSegment))
                {
                    var node = currentRow.SelectSingleNode(column.Name);
                    if (node == null)
                    {
                        row[restFunction.GetOrdinal(column)] = DBNull.Value;
                    }
                    else
                    {
                        if (node.SelectChildren(XPathNodeType.All).Count == 1 || column.Datatype == DataType.ETypeCode.Xml)
                        {
                            row[restFunction.GetOrdinal(column)] = node.OuterXml;
                        }
                        else
                        {
                            try
                            {
                                row[restFunction.GetOrdinal(column)] = DataType.TryParse(column.Datatype, node.Value);
                            }
                            catch (Exception ex)
                            {
                                throw new ConnectionException(
                                    $"Failed to convert value on column {column.Name} to datatype {column.Datatype}. {ex.Message}",
                                    ex, node.Value);
                            }
                        }
                    }
                }
                rows.Add(row);
            }

            return rows;
        }

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="filters"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<IEnumerable<object[]>> LookupRow(Table table, List<Filter> filters, CancellationToken cancellationToken)
        {
            try
            {
				var restFunction = (RestFunction)table;
                var baseRow = new object[table.Columns.Count];

                var response = await GetWebServiceResponse(restFunction, filters, cancellationToken);

                var responseStatusOrdinal = restFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseStatus);
                var responseSuccessOrdinal = restFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseSuccess);
                var responseDataOrdinal = restFunction.GetDeltaColumnOrdinal(TableColumn.EDeltaType.ResponseData);

                var lookupResult = new List<object[]>();

                if (responseStatusOrdinal >= 0)
                {
                    baseRow[responseStatusOrdinal] = response.statusCode;
                }
                if (responseSuccessOrdinal >= 0)
                {
                    baseRow[responseSuccessOrdinal] = response.isSuccess;
                }

                foreach (var column in restFunction.Columns.Where(c => c.IsInput))
                {
                    if(filters != null)
                    {
                        var filter = filters.Where(c => c.Column1.Name == column.Name);
                        if(filter.Count() == 0)
                        {
                            baseRow[restFunction.GetOrdinal(column)] = column.DefaultValue;
                        }
                        else
                        {
                            baseRow[restFunction.GetOrdinal(column)] = filter.First().Value2;
                        }
                    }
                    baseRow[restFunction.GetOrdinal(column)] = column.DefaultValue;
                }

                if(restFunction.FormatType == ETypeCode.Json)
                {
                    return ProcessJson(restFunction, baseRow, response.response);
                }

                if (restFunction.FormatType == ETypeCode.Xml)
                {
                    return ProcessXml(restFunction, baseRow, response.response);
                }

                throw new ConnectionException($"The lookup failed as the web service format type {restFunction.FormatType} is not currently supported.");
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
