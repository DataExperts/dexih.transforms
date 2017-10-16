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
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Dexih.Utils.DataType;

namespace dexih.connections.webservice
{
    public class ReaderRestful : Transform
    {
        private bool _isOpen;
        private int _cachedRow;
        private JArray _cachedJson;

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

        public override async Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;

            try
            {
                if (_isOpen)
                {
                    throw new ConnectionException($"The webservice is already open");
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
                    try
                    {
                        var result = await ReferenceTransform.Open(auditKey, null, cancelToken);
                    }
                    catch(Exception ex)
                    {
                        throw new ConnectionException($"Failed to open the input transform.", ex);
                    }
                }

                _isOpen = true;

                //create a dummy inreader to allow fieldcount and other queries to work.
                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Opening the web service reader failed. {ex.Message}", ex);
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

        public override bool ResetTransform()
        {
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            try
            {
                if(!_isOpen)
                {
                    throw new ConnectionException($"The web service is not open");
                }


                if (_cachedJson == null && await ReferenceTransform.ReadAsync(cancellationToken) == false)
                {
                    return null;
                }
                else
                {
                    var restFunction = (RestFunction)CacheTable;
                    object[] row = new object[CacheTable.Columns.Count];

                    string uri = restFunction.RestfulUri;

                    foreach (var join in JoinPairs)
                    {
                        var joinValue = join.JoinColumn == null ? join.JoinValue : ReferenceTransform[join.JoinColumn].ToString();

                        uri = uri.Replace("{" + join.SourceColumn.Name + "}", joinValue.ToString());
						row[CacheTable.GetOrdinal(join.SourceColumn.TableColumnName())] = joinValue.ToString();
                    }

                    if (_cachedJson != null)
                    {
                        var data = _cachedJson[_cachedRow];
                        for (int i = 3 + JoinPairs.Count; i < CacheTable.Columns.Count; i++)
                        {
                            object value = data.SelectToken(CacheTable.Columns[i].Name);
                            try
                            {
                                row[i] = Dexih.Utils.DataType.DataType.TryParse(CacheTable.Columns[i].Datatype, value);
                            }
                            catch(Exception ex)
                            {
                                throw new ConnectionException($"Failed to convert value on column {CacheTable.Columns[i].Name} to datatype {CacheTable.Columns[i].Datatype}. {ex.Message}", ex, value);
                            }
                        }
                        _cachedRow++;
                        if (_cachedRow >= _cachedJson.Count)
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
							var creds = new CredentialCache
							{
								{ new Uri(ReferenceConnection.Server), "basic", credentials },
								{ new Uri(ReferenceConnection.Server), "digest", credentials }
							};
							handler = new HttpClientHandler { Credentials = creds };
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
                            cancellationToken.ThrowIfCancellationRequested();

                            if (!response.IsSuccessStatusCode)
                            {
                                throw new ConnectionException($"Web service failed with status {response.StatusCode}.");
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
                                    if (_cachedJson.Count <= 1)
                                    {
                                        _cachedJson = null;
                                    }
                                }

                                for (int i = 3 + JoinPairs.Count; i < CacheTable.Columns.Count; i++)
                                {
                                    object value = data.SelectToken(CacheTable.Columns[i].Name);
                                    try
                                    {
                                        row[i] = DataType.TryParse(CacheTable.Columns[i].Datatype, value);
                                    }
                                    catch(Exception ex)
                                    {
                                        throw new ConnectionException($"Failed to convert value on column {CacheTable.Columns[i].Name} to datatype {CacheTable.Columns[i].Datatype}. {ex.Message}", ex, value);
                                    }
                                }

                            }
                        }
                    }
                    return row;
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Read web service record failed. {ex.Message}", ex);
            }
        }

        public override bool CanLookupRowDirect { get; } = true;

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        public override async Task<object[]> LookupRowDirect(List<Filter> filters, CancellationToken cancelToken)
        {
            return await ((ConnectionRestful) ReferenceConnection).LookupRow(CacheTable, filters, cancelToken);
         }
    }
}
