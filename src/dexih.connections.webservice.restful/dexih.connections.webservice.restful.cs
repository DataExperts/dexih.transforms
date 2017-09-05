﻿using System;
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
using static dexih.functions.DataType;
using System.Net;

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

        public override Task<ReturnValue> CreateTable(Table table, bool dropTable, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList(CancellationToken cancelToken)
        {
            return await Task.Run(() =>
            {
                List<string> list = new List<string>();
                return new ReturnValue<List<string>>(true, "", null, list);
            }, cancelToken);
        }

        /// <summary>
        /// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <param name="Properties">Mandatory property "RestfulUri".  Additional properties for the default column values.  Use ColumnName=value</param>
        /// <param name="importTable"></param>
        /// <param name="cancelToken"></param>
        /// <returns></returns>
        public override async Task<ReturnValue<Table>> GetSourceTableInfo(Table importTable, CancellationToken cancelToken)
        {
            try
            {
				RestFunction restFunction = (RestFunction)importTable;

                if (restFunction.RestfulUri == null )
                {
                    return new ReturnValue<Table>(false, "The RestfulUrl for the webservice has not been set.", null);
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
                    if(data.Success == false)
                    {
                        return new ReturnValue<Table>(false, data.Message, data.Exception, null);
                    }

                    TableCache reader = data.Value.Data;
                    JToken content;
                    try
                    {
                        content = JToken.Parse(reader[0][newRestFunction.GetOrdinal("Response")].ToString());
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue<Table>(false, "The following error occurred when parsing the web service result: " + ex.Message, ex, null);
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
                return new ReturnValue<Table>(true, newRestFunction);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The following error was encountered when getting the restful service information: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<List<Table>>> GetTableList(CancellationToken cancelToken)
        {
            return await Task.Run(() =>
           {
               List<Table> list = new List<Table>();
               return new ReturnValue<List<Table>>(true, "", null, list);
           }, cancelToken);
        }


        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="filters"></param>
        /// <returns></returns>
        public async Task<ReturnValue<object[]>> LookupRow(Table table, List<Filter> filters, CancellationToken cancelToken)
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
                            return new ReturnValue<object[]>(false, "Cannot perform a lookup as the result returned an array of values.", null);
                        }

                        for (int i = 3 + filters.Count; i < table.Columns.Count; i++)
                        {
                            var returnValue = DataType.TryParse(table.Columns[i].Datatype, data.SelectToken(table.Columns[i].Name));
                            if (!returnValue.Success)
                                return new ReturnValue<object[]>(returnValue);

                            row[i] = returnValue.Value;
                        }
                    }
                }

                return new ReturnValue<object[]>(true, row);
            }
            catch (Exception ex)
            {
                return new ReturnValue<object[]>(false, "The following error occurred when calling the web service: " + ex.Message, ex);
            }
        }

        public override Task<ReturnValue> TruncateTable(Table table, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<Table>> InitializeTable(Table table, int position)
        {
            return await Task.Run(() =>
            {
                var restFunction = new RestFunction();
                table.CopyProperties(restFunction, false);
                restFunction.RestfulUri = restFunction.Name;
                return new ReturnValue<Table>(true, restFunction);
            });
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

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            var lookupResult = await LookupRow(table, query.Filters, cancelToken);
            if (!lookupResult.Success)
                return new ReturnValue<object>(lookupResult);

            string schemaColumn = query.Columns[0].Column.SchemaColumnName();
            object value = lookupResult.Value[table.GetOrdinal(schemaColumn)];
            return new ReturnValue<object>(true, value);
        }

        public override Task<ReturnValue> CreateDatabase(string databaseName, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<long>> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null, bool previewMode = false)
        {
            var reader = new ReaderRestful(this, table, referenceTransform);
            return reader;
        }

        public override Task<ReturnValue<bool>> TableExists(Table table, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }


    }
}
