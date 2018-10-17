using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using dexih.functions;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Text;
using dexih.functions.File;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Dexih.Utils.Crypto;
using Dexih.Utils.MessageHelpers;

namespace dexih.connections.dexih
{
    public class ReaderDexih : Transform
    {
        private bool _isOpen = false;

        // private int[] _columnOrdinals;

		private string _dataUrl;
        
        private FileHandlerBase _fileHandler;
        private object[] _baseRow;

        private ConnectionDexih _dexihConnection;

        public ReaderDexih(Connection connection, Table table)
        {
            ReferenceConnection = connection;
            _dexihConnection = (ConnectionDexih) connection;
            CacheTable = table;
        }

        protected override void Dispose(bool disposing)
        {
            _isOpen = false;

            base.Dispose(disposing);
        }

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            try
            {
                if (_isOpen)
                {
                    throw new ConnectionException("The information hub connection is already open.");
                }

                var downloadUrl = await _dexihConnection.GetDownloadUrl();
                var intanceId = await _dexihConnection.GetRemoteAgentInstanceId();

                // call the central web server to requet the query start.
                var message = Json.SerializeObject(new
                {
                    HubName = ReferenceConnection.DefaultDatabase,
                    CacheTable.SourceConnectionName,
                    TableName = CacheTable.Name,
                    TableSchema = CacheTable.Schema,
                    Query = query,
                    DownloadUrl = downloadUrl,
                    InstanceId = intanceId
                }, "");
                
                var content = new StringContent(message, Encoding.UTF8, "application/json");
				var response = await _dexihConnection.HttpPost("OpenTableQuery", content);

                if ((bool)response["success"])
                {
                    _dataUrl = response["value"].ToString();
                }
                else
                {
                    throw new ConnectionException($"Error {response?["message"]}", new Exception(response["exceptionDetails"].ToString()));
                }
                
                // use the returned url, to start streaming the data.
                using (var httpClient = new HttpClient())
                {
                    var response2 = await httpClient.GetAsync(_dataUrl, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

                    if (response2.StatusCode == HttpStatusCode.InternalServerError)
                    {
                        var responseString = await response2.Content.ReadAsStringAsync();
                        var result = JObject.Parse(responseString);
                        var returnValue = result.ToObject<ReturnValue>();
                        throw new ConnectionException("Dexih Reader Failed.  " + returnValue.Message,
                            returnValue.Exception);
                    }

                    var responseStream = await response2.Content.ReadAsStreamAsync();
                    var config = new FileConfiguration();
                    _fileHandler = new FileHandlerText(CacheTable, config);
                    await _fileHandler.SetStream(responseStream, null);

                    _baseRow = new object[CacheTable.Columns.Count];

                    return true;
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Opening connection to information hub failed.  {ex.Message}", ex);
            }
        }

        public override string Details()
        {
            return "Information Hub Reader";
        }

        public override bool ResetTransform()
        {
            return true;
        }

 protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken)
        {
            while (true)
            {
                object[] row;
                try
                {
                    row = await _fileHandler.GetRow(_baseRow);
                }
                catch (Exception ex)
                {
                    throw new ConnectionException("The flatfile reader failed with the following message: " + ex.Message, ex);
                }

                if (row == null)
                {
                    _fileHandler.Dispose();
                }

                return row;
            }

        }
        
        //private object[] ConvertRow(IReadOnlyList<object> row)
        //{
        //    var newRow = new object[_columnOrdinals.Length];
            
        //    for(var i = 0; i < _columnOrdinals.Length; i++)
        //    {
        //        if(row[_columnOrdinals[i]] is JToken)
        //        {
        //            newRow[i] = DBNull.Value;
        //            continue;
        //        }

        //        switch(CacheTable.Columns[i].DataType)
        //        {
        //            case ETypeCode.Guid:
        //                newRow[i] = Guid.Parse(row[_columnOrdinals[i]].ToString());
        //                break;
        //           default:
        //               newRow[i] = row[_columnOrdinals[i]];
        //               break;
        //        }

        //    }

        //    return newRow;

        //}
        
        public class DataModel
        {
            public string[] Columns{ get; set; }
            public object[][] Data { get; set; }
        }


        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            throw new NotSupportedException("Direct lookup not supported with dexih connections.");
        }
    }
}
