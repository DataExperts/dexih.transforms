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
//help text for what the server means for this description
        public override string DefaultDatabaseHelp => "Service Name";
//help text for what the default database means for this description
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override bool AllowDataPoint => true;
        public override bool AllowManaged => false;
        public override bool AllowPublish => false;
        public override bool CanBulkLoad => false;
        public override string DatabaseTypeName => "Restful Web Service";
        public override ECategory DatabaseCategory => ECategory.WebService;

        public override bool CanRunQueries => false;

        public override bool PrefersSort
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        public override bool RequiresSort
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        string[]  _outputFields;

        public override Task<ReturnValue> CreateManagedTable(Table table, bool dropTable = false)
        {
            throw new NotImplementedException();
        }

        protected override ReturnValue<object[]> ReadRecord()
        {
            if (JoinTransform.Read() == false)
                return new ReturnValue<object[]>(false, null);
            else
            {
                List<Filter> filters = new List<Filter>();

                foreach (JoinPair join in JoinPairs)
                {
                    var joinValue = join.JoinColumn == null ? join.JoinValue : JoinTransform[join.JoinColumn].ToString();

                    filters.Add(new Filter()
                    {
                        Column1 = join.SourceColumn,
                        CompareDataType = ETypeCode.String,
                        Operator = Filter.ECompare.EqualTo,
                        Value2 = joinValue
                    });
                }

                var result = LookupRow(filters).Result;

                return result;
            }
        }

        public override bool CanLookupRowDirect { get; } = true;

        public override async Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters)
        { 
            try
            {
                object[] row = new object[_outputFields.Length];

                string uri = (string)CachedTable.ExtendedProperties["RestfulUri"];

                foreach (var filter in filters)
                {
                    uri = uri.Replace("{" + filter.Column1 + "}", filter.Value2.ToString());
                    row[Array.IndexOf(_outputFields, filter.Column1)] = filter.Value2.ToString();
                }

                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(ServerName);
                    client.DefaultRequestHeaders.Accept.Clear();
                    //client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                    HttpResponseMessage response = await client.GetAsync(uri);
                    row[Array.IndexOf(_outputFields, "ResponseStatusCode")] = response.StatusCode.ToString();
                    row[Array.IndexOf(_outputFields, "ResponseSuccess")] = response.IsSuccessStatusCode;
                    row[Array.IndexOf(_outputFields, "Response")] = await response.Content.ReadAsStringAsync();

                    if (_outputFields.Length > 3 + filters.Count)
                    {
                        JObject data = JObject.Parse(row[Array.IndexOf(_outputFields, "Response")].ToString());

                        for (int i = 3 + filters.Count; i < _outputFields.Length; i++)
                        {
                            row[i] = data.SelectToken(_outputFields[i]);
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

        protected override async Task<ReturnValue> DataReaderStartQueryInner(Table table, SelectQuery query)
        {
            return await Task.Run(() =>
            {
                try
                {
                    if (OpenReader)
                    {
                        return new ReturnValue(false, "The web service connection is already open.", null);
                    }

                    _outputFields = table.Columns.Select(c => c.ColumnName).ToArray();

                    //if no driving table is set, then use the row creator to simulate a single row.
                    if (JoinTransform == null)
                    {
                        SourceRowCreator rowCreator = new SourceRowCreator();
                        rowCreator.InitializeRowCreator(1, 1, 1);
                        JoinTransform = rowCreator;
                    }

                    CachedTable = table;

                    //create a dummy inreader to allow fieldcount and other queries to work.
                    return new ReturnValue(true);
                }
                catch (Exception ex)
                {
                    return new ReturnValue(false, "The following error occurred when starting the web service: " + ex.Message, ex);
                }
            });
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
                table.ExtendedProperties.Add("RestfulUri", restfulUri);

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
                        content = JObject.Parse(reader[0][Array.IndexOf(_outputFields, "Response")].ToString());
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


        public override bool IsClosed => false;

        public override bool NextResult()
        {
            return Read();
        }


        public override Task<ReturnValue> TruncateTable(Table table)
        {
            throw new NotImplementedException();
        }

        public override string GetCurrentFile()
        {
            throw new NotImplementedException();
        }

        public override ReturnValue ResetTransform()
        {
            throw new NotImplementedException();
        }

        public override bool Initialize()
        {
            throw new NotImplementedException();
        }

        public override string Details()
        {
            return "Web Service: " + CachedTable.TableName;
        }

        public override Task<ReturnValue> AddMandatoryColumns(Table table, int position)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteUpdateQuery(Table table, List<UpdateQuery> query)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteDeleteQuery(Table table, List<DeleteQuery> query)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteInsertQuery(Table table, List<InsertQuery> query)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query)
        {
            throw new NotImplementedException();
        }

        public override async Task<ReturnValue> DataWriterStart(Table table)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public override async Task<ReturnValue> DataWriterFinish(Table table)
        {
            return await Task.Run(() => new ReturnValue(true));
        }

        public override Task<ReturnValue> CreateDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }
    }
}
