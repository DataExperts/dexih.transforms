using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using dexih.functions;
using System.Net.Http;

using System.Threading;
using System.Text;
using System.Text.Json;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using dexih.transforms.File;
using Dexih.Utils.MessageHelpers;

namespace dexih.connections.dexih
{
    public class ReaderDexih : Transform
    {
		private string _dataUrl;
        private bool _isFirst = true;
        
        private FileHandlerBase _fileHandler;

        private readonly ConnectionDexih _dexihConnection;

        public ReaderDexih(Connection connection, Table table)
        {
            ReferenceConnection = connection;
            _dexihConnection = (ConnectionDexih) connection;
            CacheTable = table;
        }

        public override string TransformName { get; } = "Integration Hub Reader";
        public override Dictionary<string, object> TransformProperties()
        {
            return null;
        }

        public override async Task<bool> Open(long auditKey, SelectQuery requestQuery = null, CancellationToken cancellationToken = default)
        {
            AuditKey = auditKey;

            try
            {
                if (IsOpen)
                {
                    throw new ConnectionException("The integration hub connection is already open.");
                }

                IsOpen = true;
                SelectQuery = requestQuery;
                GeneratedQuery = SelectQuery;

                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Opening connection to integration hub failed.  {ex.Message}", ex);
            }
        }


        public override bool ResetTransform()
        {
            return true;
        }

        protected override async Task<object[]> ReadRecord(CancellationToken cancellationToken = default)
        {
            if (_isFirst)
            {
                var downloadUrl = await _dexihConnection.GetDownloadUrl();
                var instanceId = await _dexihConnection.GetRemoteAgentInstanceId();

                // call the central web server to request the query start.
                var message = new
                {
                    HubName = ReferenceConnection.DefaultDatabase,
                    CacheTable.SourceConnectionName,
                    TableName = CacheTable.Name,
                    TableSchema = CacheTable.Schema,
                    Query = SelectQuery,
                    DownloadUrl = downloadUrl,
                    InstanceId = instanceId
                }.Serialize();

                var content = new StringContent(message, Encoding.UTF8, "application/json");
                _dataUrl = await _dexihConnection.HttpPostRaw("OpenTableQuery", content);

                var httpClient = _dexihConnection.ClientFactory.CreateClient();
                httpClient.Timeout = TimeSpan.FromMinutes(2);
                var response = await httpClient.GetAsync(downloadUrl.Url + "/download/" +_dataUrl, HttpCompletionOption.ResponseHeadersRead,
                    cancellationToken);

                if (response.StatusCode == HttpStatusCode.InternalServerError)
                {
                    var stream = await response.Content.ReadAsStreamAsync();
                    var returnValue = await stream.DeserializeAsync<ReturnValue>(cancellationToken);
                    throw new ConnectionException("Dexih Reader Failed.  " + returnValue.Message, returnValue.Exception);
                }

                if (!response.IsSuccessStatusCode)
                {
                    if (response.Content.Headers.ContentType.MediaType == "application/json")
                    {
                        var stream = await response.Content.ReadAsStreamAsync();
                        var returnValue = await stream.DeserializeAsync<ReturnValue>(cancellationToken);
                        throw new ConnectionException("Dexih Reader Failed.  " + returnValue.Message,
                            returnValue.Exception);
                    }
                    else
                    {
                        throw new ConnectionException(
                            $"Dexih Reader Failed to connect.  Code: {response.StatusCode}, Reason: {response.ReasonPhrase}.  Review logs on the target hubs remote server for more information.");
                    }
                }

                var responseStream = await response.Content.ReadAsStreamAsync();
                var config = new FileConfiguration();
                _fileHandler = new FileHandlerText(CacheTable, config);
                await _fileHandler.SetStream(responseStream, null);
                _isFirst = false;
            }
            
            while (true)
            {
                object[] row;
                try
                {
                    row = await _fileHandler.GetRow();
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


        public override Task<bool> InitializeLookup(long auditKey, SelectQuery query, CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException("Direct lookup not supported with dexih connections.");
        }
    }
}
