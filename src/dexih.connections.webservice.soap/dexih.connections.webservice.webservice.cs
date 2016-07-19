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
using System.Threading;

#if NET46
using System.Web.Services.Description;
#endif

namespace dexih.connections.webservice
{
    
    public class ConnectionSoap : Connection
    {
        public override string ServerHelp => "The full path (including http://) of the Web Service Description Language (WSDL) file.";
        public override string DefaultDatabaseHelp => "Service Name";
        public override bool AllowNtAuth => true;
        public override bool AllowUserPass => true;
        public override bool CanBulkLoad => false;
        public override bool CanSort => false;
        public override bool CanFilter => false;
        public override bool CanAggregate => false;


        public override string DatabaseTypeName => "SOAP/Xml Web Service";
        public override ECategory DatabaseCategory => ECategory.WebService;


#if NET46

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

        public async Task<ReturnValue<object>> GetWebService()
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

        public async Task<ReturnValue<object[]>> LookupRow(Table table, List<Filter> filters, Type webServiceType, object webServiceObject)
        {
            try
            {
                object[] row = new object[table.Columns.Count];

                object[] args = new object[filters.Count()];

                for (int i = 0; i < filters.Count; i++)
                {
                    row[table.GetOrdinal(filters[i].Column1)] = filters[i].Value2;
                    args[i] = filters[i].Value2;
                }

                object result = await Task.Run(() => webServiceType.InvokeMember(table.TableName, BindingFlags.InvokeMethod, null, webServiceObject, args));
                if (result == null)
                {
                    return new ReturnValue<object[]>(false, "Error: The web service call did not return a result.", null);
                }
                else
                {
                    Type resultType = result.GetType();
                    if (resultType.Name == "String")
                    {
                        row[table.GetOrdinal("Result")] = result.ToString();
                    }
                    else
                    {
                        foreach (FieldInfo field in resultType.GetFields())
                        {
                            row[table.GetOrdinal(field.Name)] = field.GetValue(result);
                        }
                        foreach (PropertyInfo property in resultType.GetProperties())
                        {
                            row[table.GetOrdinal(property.Name)] = property.GetValue(result);
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

        public override async Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, string> Properties)
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
        public Task<ReturnValue<object>> GetWebService()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<List<string>>> GetDatabaseList()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<List<string>>> GetTableList()
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<Table>> GetSourceTableInfo(string tableName, Dictionary<string, string> Properties)
        {
            throw new NotImplementedException();
        }

        public Task<ReturnValue<object[]>> LookupRow(Table table, List<Filter> filters, Type webServiceType, object webServiceObject)
        {
            throw new NotImplementedException();
        }

#endif

        public override Task<ReturnValue> CreateTable(Table table, bool dropTable = false)
        {
            throw new NotImplementedException();
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

        public override Task<ReturnValue<object>> ExecuteScalar(Table table, SelectQuery query, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }


        public override Task<ReturnValue> CreateDatabase(string databaseName)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<DbDataReader>> GetDatabaseReader(Table table, DbConnection connection, SelectQuery query = null)
        {
            throw new NotImplementedException();
        }

        public override Task<ReturnValue<int>> ExecuteInsertBulk(Table table, DbDataReader sourceData, CancellationToken cancelToken)
        {
            throw new NotImplementedException();
        }

        public override Transform GetTransformReader(Table table, Transform referenceTransform = null, List<JoinPair> referenceJoins = null)
        {
            var reader = new ReaderWebService(this, table, referenceTransform, referenceJoins);
            return reader;
        }


    }
}
