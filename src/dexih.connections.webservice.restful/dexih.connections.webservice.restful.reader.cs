using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using dexih.functions;
using System.Threading;
using System.Linq;
using System.Net.Http;
using System.Net;
using Newtonsoft.Json.Linq;

namespace dexih.connections.webservice
{
    public class ReaderRestful : Transform
    {
        private bool _isOpen = false;

        private int _cachedRow = 0;
        private JArray _cachedJson = null;

        public ReaderRestful(Connection connection, Table table, Transform referenceTransform)
        {
            ReferenceConnection = connection;
            CacheTable = table;
            ReferenceTransform = referenceTransform;
        }

        protected override void Dispose(bool disposing)
        {
            _isOpen = false;

            base.Dispose(disposing);
        }

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;

            try
            {
                if (_isOpen)
                {
                    return new ReturnValue(false, "The web service connection is already open.", null);
                }

                //if no driving table is set, then use the row creator to simulate a single row.
                if (ReferenceTransform == null)
                {
                    ReaderRowCreator rowCreator = new ReaderRowCreator();
                    rowCreator.InitializeRowCreator(1, 1, 1);
                    base.ReferenceTransform = rowCreator;
                }
                else
                {
                    var result = await ReferenceTransform.Open(auditKey, null, cancelToken);
                    if (!result.Success)
                        return result;
                }

                _isOpen = true;

                //create a dummy inreader to allow fieldcount and other queries to work.
                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when starting the web service: " + ex.Message, ex);
            }
        }

        public override string Details()
        {
            return "Restful WebService";
        }

        public override bool InitializeOutputFields()
        {
            return true;
        }

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            try
            {
                if(!_isOpen)
                {
                    return new ReturnValue<object[]>(false, "The read record failed as the reader has not been opened.", null);
                }


                if (_cachedJson == null && await ReferenceTransform.ReadAsync(cancellationToken) == false)
                    return new ReturnValue<object[]>(false, null);
                else
                {
                    var restFunction = (RestFunction)CacheTable;
                    object[] row = new object[CacheTable.Columns.Count];

                    string uri = restFunction.RestfulUri;

                    foreach (var join in JoinPairs)
                    {
                        var joinValue = join.JoinColumn == null ? join.JoinValue : ReferenceTransform[join.JoinColumn].ToString();

                        uri = uri.Replace("{" + join.SourceColumn.Name + "}", joinValue.ToString());
                        row[CacheTable.GetOrdinal(join.SourceColumn.SchemaColumnName())] = joinValue.ToString();
                    }

                    if (_cachedJson != null)
                    {
                        var data = _cachedJson[_cachedRow];
                        for (int i = 3 + JoinPairs.Count; i < CacheTable.Columns.Count; i++)
                        {
                            var returnValue = DataType.TryParse(CacheTable.Columns[i].Datatype, data.SelectToken(CacheTable.Columns[i].Name));
                            if (!returnValue.Success)
                                return new ReturnValue<object[]>(returnValue);

                            row[i] = returnValue.Value;
                        }
                        _cachedRow++;
                        if(_cachedRow >= _cachedJson.Count)
                        {
                            _cachedJson = null;
                        }
                    }
                    else
                    {

                        foreach (var column in CacheTable.Columns.Where(c => c.IsInput))
                        {
                            if (column.DefaultValue != null)
                            {
                                uri = uri.Replace("{" + column.Name + "}", column.DefaultValue);
                            }
                        }

                        HttpClientHandler handler = null;
                        if (!String.IsNullOrEmpty(ReferenceConnection.Username))
                        {
                            var credentials = new NetworkCredential(ReferenceConnection.Username, ReferenceConnection.Password);
                            var creds = new CredentialCache();
                            creds.Add(new Uri(ReferenceConnection.Server), "basic", credentials);
                            creds.Add(new Uri(ReferenceConnection.Server), "digest", credentials);

                            handler = new HttpClientHandler { Credentials = creds };

                            handler = new HttpClientHandler { Credentials = credentials };
                        }
                        else
                        {
                            handler = new HttpClientHandler();
                        }

                        using (var client = new HttpClient(handler))
                        {
                            client.BaseAddress = new Uri(ReferenceConnection.Server);
                            client.DefaultRequestHeaders.Accept.Clear();
                            //client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                            HttpResponseMessage response = await client.GetAsync(uri, cancellationToken);
                            if (cancellationToken.IsCancellationRequested)
                            {
                                return new ReturnValue<object[]>(false, "Reader was cancelled", null);
                            }
                            if (!response.IsSuccessStatusCode)
                            {
                                return new ReturnValue<object[]>(false, "Webservice called failed with status: " + response.StatusCode.ToString(), null);
                            }

                            row[CacheTable.GetOrdinal("ResponseStatusCode")] = response.StatusCode.ToString();
                            row[CacheTable.GetOrdinal("ResponseSuccess")] = response.IsSuccessStatusCode;
                            row[CacheTable.GetOrdinal("Response")] = await response.Content.ReadAsStringAsync();

                            if (CacheTable.Columns.Count > 3 + JoinPairs.Count)
                            {
                                JToken data = JToken.Parse(row[CacheTable.GetOrdinal("Response")].ToString());

                                if (data.Type == JTokenType.Array)
                                {
                                    _cachedJson = (JArray)data;
                                    data = _cachedJson[0];
                                    _cachedRow = 1;
                                    if(_cachedJson.Count <= 1)
                                    {
                                        _cachedJson = null;
                                    }
                                }

                                for (int i = 3 + JoinPairs.Count; i < CacheTable.Columns.Count; i++)
                                {
                                    var returnValue = DataType.TryParse(CacheTable.Columns[i].Datatype, data.SelectToken(CacheTable.Columns[i].Name));
                                    if (!returnValue.Success)
                                        return new ReturnValue<object[]>(returnValue);

                                    row[i] = returnValue.Value;
                                }

                            }
                        }
                    }
                    return new ReturnValue<object[]>(true, row);
                }
            }
            catch (Exception ex)
            {
                throw new Exception("The restful service failed due to the following error: " + ex.Message, ex);
            }
        }

        public override bool CanLookupRowDirect { get; } = true;

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public override async Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters, CancellationToken cancelToken)
        {
            return await ((ConnectionRestful) ReferenceConnection).LookupRow(CacheTable, filters, cancelToken);
         }
    }
}
