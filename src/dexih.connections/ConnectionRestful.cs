using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using dexih.transforms;
using dexih.functions;
using System.IO;
using System.Data.Common;
using System.Globalization;
using System.Text.RegularExpressions;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Threading;
using static dexih.functions.DataType;

namespace dexih.connections
{
    
    public class ConnectionRestful : Connection
    {
        public override string ServerHelp => "The API end point for the Restful web service, excluding query strings.  Eg.  http://twitter.com/statuses/";
        public override string DefaultDatabaseHelp => "Service Name";
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override bool CanBulkLoad => false;
        public override string DatabaseTypeName => "Restful Web Service";
        public override ECategory DatabaseCategory => ECategory.WebService;

        public override Task<ReturnValue> CreateTable(Table table, bool dropTable = false)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            return await Task.Run(() =>
            {
                List<string> list = new List<string>();
                return new ReturnValue<List<string>>(true, "", null, list);
            });
        }

        /// <summary>
        /// Retrieves web services information.  The RestfulUri must be passed through the properties.  This should be in the format http://sitename/{value1}/{value2} where the names between {} are the names for input parameters.  The properties can also contain default values for the parameters.
        /// </summary>
        /// <param name="tableName">Table Name</param>
        /// <param name="Properties">Mandatory property "RestfulUri".  Additional properties for the default column values.  Use ColumnName=value</param>
        /// <returns></returns>
         public override async Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, object> Properties)
        {
            try
            {
                if (Properties == null || Properties["RestfulUri"] == null || !(Properties["FileFormat"] is string) )
                {
                    return new ReturnValue<Table>(false, "The properties have not been set to Restful Web Service.  Required properties are (string)RestfulUri.", null);
                }

                string restfulUri = (string)Properties["RestfulUri"];

                Table table = new Table(tableName);
                table.TableName = table.TableName;

                //The new datatable that will contain the table schema
                table.Columns.Clear();
                table.Description = "";
                table.SetExtendedProperty("RestfulUri", restfulUri);

                table.LogicalName = table.TableName;

                TableColumn col;

                //use the regex to extract items in uri between { }.  These will be input columns
                Match match = Regex.Match(restfulUri, @"\{([^}]+)\}");

                while (match.Success)
                {
                    string name = match.Groups[1].Value;

                    col = new TableColumn();

                    //add the basic properties
                    col.ColumnName = name;
                    col.IsInput = true;
                    col.LogicalName = name;
                    col.DataType = ETypeCode.String;
                    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                    col.MaxLength = 1024;

                    col.Description = "Url Parameter " + name;

                    col.AllowDbNull = true;
                    col.IsUnique = false;

                    //Copy the inputvalue from the table input.  This allows the preview table function below to get sample data.
                    if (Properties.ContainsKey(name))
                    {
                        col.ExtendedProperties.Add("InputValue", Properties["name"]);
                    }

                    table.Columns.Add(col);
                    match = match.NextMatch();
                }


                //This column is use to capture the entire response from the web services call.
                col = new TableColumn()
                {
                    ColumnName = "Response",
                    IsInput = false,
                    LogicalName = "Response",
                    DataType = ETypeCode.String,
                    DeltaType = TableColumn.EDeltaType.TrackingField,
                    MaxLength = null,
                    Description = "Response content from the service",
                    AllowDbNull = true,
                    IsUnique = false
                };
                table.Columns.Add(col);

                col = new TableColumn()
                {
                    ColumnName = "ResponseStatusCode",
                    IsInput = false,
                    LogicalName = "ResponseStatusCode",
                    DataType = ETypeCode.String,
                    DeltaType = TableColumn.EDeltaType.TrackingField,
                    MaxLength = null,
                    Description = "The status code returned by the service",
                    AllowDbNull = true,
                    IsUnique = false
                };
                table.Columns.Add(col);

                col = new TableColumn()
                {
                    ColumnName = "ResponseSuccess",
                    IsInput = false,
                    LogicalName = "ResponseSuccess",
                    DataType = ETypeCode.Boolean,
                    DeltaType = TableColumn.EDeltaType.TrackingField,
                    MaxLength = null,
                    Description = "Is the web service call successful.",
                    AllowDbNull = true,
                    IsUnique = false
                };
                table.Columns.Add(col);

                SelectQuery query = new SelectQuery();
                query.Columns.Add(new SelectColumn("Response", SelectColumn.EAggregate.None));
                query.Columns.Add(new SelectColumn("ResponseSuccess", SelectColumn.EAggregate.None));
                query.Table = table.TableName;
                query.Rows = 1;

                if (table.Columns.Count > 0)
                {
                    var ts = new CancellationTokenSource();
                    CancellationToken ct = ts.Token;

                    var data = await GetPreview(table, query, 10000, ct);
                    if(data.Success == false)
                    {
                        return new ReturnValue<Table>(false, data.Message, data.Exception, null);
                    }

                    TableCache reader = data.Value.Data;
                    JObject content;
                    try
                    {
                        content = JObject.Parse(reader[0][table.GetOrdinal("Response")].ToString());
                    }
                    catch (Exception ex)
                    {
                        return new ReturnValue<Table>(false, "The following error occurred when parsing the web service result: " + ex.Message, ex, null);
                    }

                    if (content != null)
                    {
                        foreach (var value in content.Children())
                        {
                            col = new TableColumn();
                            col.ColumnName = value.Path;
                            col.IsInput = false;
                            col.LogicalName = value.Path;
                            col.DataType = ETypeCode.String;
                            col.DeltaType = TableColumn.EDeltaType.TrackingField;
                            col.MaxLength = null;
                            col.Description = "Json value of the " + value.Path + " path";
                            col.AllowDbNull = true;
                            col.IsUnique = false;
                            table.Columns.Add(col);
                        }
                    }
                }
                return new ReturnValue<Table>(true, table);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The following error was encountered when getting the restful service information: " + ex.Message, ex);
            }
        }

        public override async Task<ReturnValue<List<string>>> GetTableList()
        {
            return await Task.Run(() =>
           {
               List<string> list = new List<string>();
               return new ReturnValue<List<string>>(true, "", null, list);
           });
        }


        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public async Task<ReturnValue<object[]>> LookupRow(Table table, List<Filter> filters)
        {
            try
            {
                object[] row = new object[table.Columns.Count];

                string uri = (string)table.GetExtendedProperty("RestfulUri");

                foreach (var filter in filters)
                {
                    uri = uri.Replace("{" + filter.Column1 + "}", filter.Value2.ToString());
                    row[table.GetOrdinal(filter.Column1)] = filter.Value2.ToString();
                }

                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(ServerName);
                    client.DefaultRequestHeaders.Accept.Clear();
                    //client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    HttpResponseMessage response = await client.GetAsync(uri);

                    row[table.GetOrdinal("ResponseStatusCode")] = response.StatusCode.ToString();
                    row[table.GetOrdinal("ResponseSuccess")] = response.IsSuccessStatusCode;
                    row[table.GetOrdinal("Response")] = await response.Content.ReadAsStringAsync();

                    if (table.Columns.Count > 3 + filters.Count)
                    {
                        JObject data = JObject.Parse(row[table.GetOrdinal("Response")].ToString());

                        for (int i = 3 + filters.Count; i < table.Columns.Count; i++)
                        {
                            row[i] = data.SelectToken(table.Columns[i].ColumnName);
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

        public override async Task<ReturnValue> AddMandatoryColumns(Table table, int position)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public override Task<ReturnValue<int>> ExecuteUpdate(Table table, List<UpdateQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteDelete(Table table, List<DeleteQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteInsert(Table table, List<InsertQuery> query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            var lookupResult = await LookupRow(table, query.Filters);
            if (!lookupResult.Success)
                return new ReturnValue<object>(lookupResult);

            string column = query.Columns[0].Column;
            object value = lookupResult.Value[table.GetOrdinal(column)];
            return new ReturnValue<object>(true, value);
        }

        public override Task<ReturnValue> CreateDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, SelectQuery query = null)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null)
        {
            var reader = new ReaderRestful(this, table, referenceTransform);
            return reader;
        }

    }
}
