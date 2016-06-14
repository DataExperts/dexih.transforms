using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Reflection;
using dexih.transforms;
using dexih.functions;
using System.IO;
using System.Data.Common;
using System.Globalization;
using static dexih.functions.DataType;

#if NET451
using System.Web.Services.Description;
#endif

namespace dexih.connections
{
    
    public class ConnectionWebService : Connection
    {
        public override string ServerHelp => "The full path (including http://) of the Web Service Description Language (WSDL) file.";
        public override string DefaultDatabaseHelp => "Service Name";
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override bool AllowDataPoint => true;
        public override bool AllowManaged => false;
        public override bool AllowPublish => false;
        public override bool CanBulkLoad => false;
        public override string DatabaseTypeName => "SOAP/Xml Web Service";
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


#if NET451
        private Type _webServiceType;
        private string[] _outputFields;
        private object  _webServiceObject;


        private async Task<ReturnValue<ServiceDescription>> GetServiceDescription()
        {
            try
            {
                System.Net.WebClient client = new System.Net.WebClient();
                Stream stream = await client.OpenReadTaskAsync(ServerName);
                // Get a WSDL file describing a service.
                ServiceDescription description = await Task.Run(() => ServiceDescription.Read(stream));

                return new ReturnValue<ServiceDescription>(true, "", null,description);
            }
            catch(Exception ex)
            {
                return new ReturnValue<ServiceDescription>(false, "The following error was returned retrieving a service description from the web service: " + ex.Message, ex, null);
            }
        }

        private async Task<ReturnValue<object>> GetWebService()
        {
            var serviceDescription = await GetServiceDescription();
            if(serviceDescription.Success == false)
            {
                return new ReturnValue<object>(serviceDescription.Success, serviceDescription.Message, serviceDescription.Exception, null);
            }

            return await Task.Run(() =>
            {
                try
                {
                    // Initialize a service description importer.
                    // Use SOAP 1.2.
                    ServiceDescriptionImporter importer = new ServiceDescriptionImporter {ProtocolName = "Soap12"};
                    
                    importer.AddServiceDescription(serviceDescription.Value, null, null);

                    // Generate a proxy client.
                    importer.Style = ServiceDescriptionImportStyle.Client;

                    // Generate properties to represent primitive values.
                    importer.CodeGenerationOptions = System.Xml.Serialization.CodeGenerationOptions.GenerateProperties;

                    // Initialize a Code-DOM tree into which we will import the service.
                    CodeNamespace nmspace = new CodeNamespace();
                    CodeCompileUnit unit1 = new CodeCompileUnit();
                    unit1.Namespaces.Add(nmspace);

                    // Import the service into the Code-DOM tree. This creates proxy code
                    // that uses the service.
                    ServiceDescriptionImportWarnings warning = importer.Import(nmspace, unit1);

                    if (warning == 0)
                    {
                        // Generate and print the proxy code in C#.
                        CodeDomProvider provider1 = CodeDomProvider.CreateProvider("CSharp");

                        // Compile the assembly with the appropriate references
                        string[] assemblyReferences = new string[] { "System.Web.Services.dll", "System.Xml.dll" };
                        CompilerParameters parms = new CompilerParameters(assemblyReferences);
                        CompilerResults results = provider1.CompileAssemblyFromDom(parms, unit1);

                        string errors = "";
                        foreach (CompilerError oops in results.Errors)
                        {
                            errors += oops.ErrorText;
                        }
                        if (errors != "")
                        {
                            State = EConnectionState.Broken;
                            return new ReturnValue<object>(false, "The following errors occurred importing the web service: " + errors, null, null);
                        }

                        //Invoke the web service method
                        object o = results.CompiledAssembly.CreateInstance(DefaultDatabase);
                        State = EConnectionState.Open;
                        return new ReturnValue<object>(true, "", null , o);
                    }

                    State = EConnectionState.Broken;
                    return new ReturnValue<object>(false, "The following warning occurred importing the web service: " + warning.ToString(), null, null);

                }
                catch (Exception ex)
                {
                    State = EConnectionState.Broken;
                    return new ReturnValue<object>(false, "The following error occurred importing the web service: " + ex.Message, ex, null);
                }
            });
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

                object[] args = new object[filters.Count()];

                for (int i = 0; i < filters.Count; i++)
                {
                    row[Array.IndexOf(_outputFields, filters[i].Column1)] = filters[i].Value2;
                    args[i] = filters[i].Value2;
                }

                object result = await Task.Run(() => _webServiceType.InvokeMember(CachedTable.TableName, BindingFlags.InvokeMethod, null, _webServiceObject, args));
                if (result == null)
                {
                    return new ReturnValue<object[]>(false, "Error: The web service call did not return a result.", null);
                }
                else
                {
                    Type resultType = result.GetType();
                    if (resultType.Name == "String")
                    {
                        row[Array.IndexOf(_outputFields, "Result")] = result.ToString();
                    }
                    else
                    {
                        foreach (FieldInfo field in resultType.GetFields())
                        {
                            row[Array.IndexOf(_outputFields, field.Name)] = field.GetValue(result);
                        }
                        foreach (PropertyInfo property in resultType.GetProperties())
                        {
                            row[Array.IndexOf(_outputFields, property.Name)] = property.GetValue(result);
                        }
                    }

                    return new ReturnValue<object[]>(true, row);
                }
            }
            catch(Exception ex)
            {
                return new ReturnValue<object[]>(false, "The following error occurred when calling the web service: " + ex.Message, ex);
            }
        }

        protected override async Task<ReturnValue> DataReaderStartQueryInner(Table table, SelectQuery query)
        {
            try
            {
                if (OpenReader)
                {
                    return new ReturnValue(false, "The web service connection is already open.", null);
                }

                var wsResult = await GetWebService();
                if (wsResult.Success == false)
                    return wsResult;

                _webServiceObject = wsResult.Value;
                _webServiceType = _webServiceObject.GetType();

                _outputFields = table.Columns.Select(c => c.ColumnName).ToArray();

                //if no driving table is set, then use the row creator to simulate a single row.
                if (JoinTransform == null)
                {
                    SourceRowCreator rowCreator = new SourceRowCreator();
                    rowCreator.InitializeRowCreator(1, 1, 1);
                    JoinTransform = rowCreator;
                }

                CachedTable = table;

                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when starting the web service: " + ex.Message, ex);
            }

        }

        public override async Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            try
            {
                List<string> list = new List<string>();

                var serviceDescription = await GetServiceDescription();
                if (serviceDescription.Success == false)
                    return new ReturnValue<List<string>>(false, serviceDescription.Message, serviceDescription.Exception, null);

                foreach (Service service in serviceDescription.Value.Services)
                {
                    list.Add(service.Name);
                }

                return new ReturnValue<List<string>>(true, "", null,  list.OrderBy(c => c).ToList());
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The following error occurred when starting the web service: " + ex.Message, ex, null);
            }
        }

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, object> Properties)
        {
            try
            {
                var webService = await GetWebService();
                if (webService.Success == false)
                    return new ReturnValue<Table>(false, webService.Message, webService.Exception, null);

                Type t = webService.Value.GetType();

                //The new datatable that will contain the table schema
                Table table = new Table(tableName);
                table.Description = "";
                table.LogicalName = tableName;

                TableColumn col;

                MethodInfo method = t.GetMethod(table.TableName);
                
                foreach (ParameterInfo parameter in method.GetParameters())
                {
                    col = new TableColumn()
                    {
                        ColumnName = parameter.Name,
                        IsInput = true,
                        LogicalName = parameter.Name
                    };

                    //add the basic properties
                    if (parameter.ParameterType.IsGenericType)
                        col.ColumnGetType = parameter.ParameterType;
                    else
                        col.DataType = ETypeCode.String;

                    col.DeltaType = TableColumn.EDeltaType.NaturalKey;
                    col.MaxLength = 1024;

                    //add the description (haven't worked out how to get from database)
                    col.Description = "";

                    col.AllowDbNull = true;
                    col.IsUnique = false;
                    table.Columns.Add(col);
                }

                switch (method.ReturnType.Name) {
                    case "Byte":
                    case "Int16":
                    case "Int32":
                    case "Double":
                    case "Decimal":
                    case "String":
                    case "Boolean":
                    case "Guid":
                    case "TimeSpan":
                    case "DateTime":
                        col = new TableColumn();

                        //add the basic properties
                        col.ColumnName = "Result";
                        col.IsInput = false;
                        col.LogicalName = "Result";
                        col.ColumnGetType = Type.GetType("System." + method.ReturnType.Name);
                        col.DeltaType = TableColumn.EDeltaType.TrackingField;

                        //add the description (haven't worked out how to get from database)
                        col.Description = "";

                        col.AllowDbNull = true;
                        col.IsUnique = false;
                        table.Columns.Add(col);
                        break;
                    default:
                        foreach (FieldInfo field in method.ReturnType.GetFields())
                            {
                                col = new TableColumn();

                                //add the basic properties
                                col.ColumnName = field.Name;
                                col.IsInput = false;
                                col.LogicalName = field.Name;
                                col.ColumnGetType = field.FieldType;
                                col.DeltaType = TableColumn.EDeltaType.TrackingField;

                                //add the description (haven't worked out how to get from database)
                                col.Description = "";

                                col.AllowDbNull = true;
                                col.IsUnique = false;
                                table.Columns.Add(col);
                            }

                            foreach (PropertyInfo property in method.ReturnType.GetProperties())
                            {
                                col = new TableColumn();

                                //add the basic properties
                                col.ColumnName = property.Name;
                                col.IsInput = false;
                                col.LogicalName = property.Name;
                                col.ColumnGetType = property.PropertyType;
                                col.DeltaType = TableColumn.EDeltaType.TrackingField;

                                //add the description (haven't worked out how to get from database)
                                col.Description = "";

                                col.AllowDbNull = true;
                                col.IsUnique = false;
                                table.Columns.Add(col);
                            }
                            break;
                }

                return new ReturnValue<Table>(true, table);
            }
            catch (Exception ex)
            {
                return new ReturnValue<Table>(false, "The following error was encountered when getting the web service information: " + ex.Message, ex);
            }
    }

        public override async Task<ReturnValue<List<string>>> GetTableList()
        {
            try
            {
                List<string> list = new List<string>();

                var webService = await GetWebService();
                if (webService.Success == false)
                    return new ReturnValue<List<string>>(false, webService.Message, webService.Exception, null);

                Type t = webService.Value.GetType();

                foreach (MethodInfo method in t.GetMethods())
                {
                    if (method.Name == "Discover") // this is to reduce bringing in all generic web service methods
                        break;
                    list.Add(method.Name);
                }

                return new ReturnValue<List<string>>(true, "", null, list);
            }
            catch (Exception ex)
            {
                return new ReturnValue<List<string>>(false, "The following error was encountered when getting the web service methods: " + ex.Message, ex, null);
            }
        }
#else
        public override Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<List<string>>> GetTableList()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, object> Properties)
        {
            throw new NotImplementedException();
        }

        protected override Task<ReturnValue> DataReaderStartQueryInner(Table table, SelectQuery query)
        {
            throw new NotImplementedException();
        }

        protected override ReturnValue<object[]> ReadRecord()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<object[]>> LookupRow(List<Filter> filters)
        {
            throw new NotImplementedException();
        }
#endif

        public override Task<ReturnValue> CreateManagedTable(Table table, bool dropTable = false)
        {
            throw new NotImplementedException();
        }


        public override bool IsClosed => false;

        public override int FieldCount => CachedTable.Columns.Count;

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



        public override async Task<ReturnValue> AddMandatoryColumns(Table table, int position)
        {
            return await Task.Run(() => new ReturnValue(true));
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
