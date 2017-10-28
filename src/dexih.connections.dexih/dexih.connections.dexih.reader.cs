using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Text;
using dexih.functions.Query;
using dexih.transforms.Exceptions;
using Dexih.Utils.Crypto;
using static Dexih.Utils.DataType.DataType;

namespace dexih.connections.dexih
{
    public class ReaderDexih : Transform
    {
        private bool _isOpen = false;

		private int _datasetRow;
        private string[] _columns;
        private int[] _columnOrdinals;
        private object[][] _dataset;
		private bool _moreData;
		private string _continuationToken;

        public ReaderDexih(Connection connection, Table table, Transform referenceTransform)
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

        public override async Task<bool> Open(long auditKey, SelectQuery query, CancellationToken cancellationToken)
        {
            AuditKey = auditKey;

            try
            {
                if (_isOpen)
                {
                    throw new ConnectionException("The information hub connection is already open.");
                }

                var message = Json.SerializeObject(new
                {
                    HubName = ReferenceConnection.DefaultDatabase,
                    SourceConnectionName = CacheTable.SourceConnectionName,
                    TableName = CacheTable.Name,
                    TableSchema = CacheTable.Schema,
                    Query = query,
                }, "");

                var content = new StringContent(message, Encoding.UTF8, "application/json");
				var response = await ((ConnectionDexih)ReferenceConnection).HttpPost("OpenTableQuery", content, true);


                if ((bool)response["success"])
                {
                    _continuationToken = response["value"].ToString();
                }
                else
                {
                    throw new ConnectionException($"Error {response?["message"]}", new Exception(response["exceptionDetails"].ToString()));
                }

				_datasetRow = 0;
				_moreData = true;

                return true;
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
                // if we already have a cached dataset, then keep reading
                if (_dataset != null && _datasetRow < _dataset.Count())
				{
					var row = ConvertRow(_dataset[_datasetRow]);
                    _datasetRow++;

                    return row;
				}
                
                if(!_moreData)
                {
                    _isOpen = false;
                    return null;
                }
                
                //var message = Json.SerializeObject(new { ContinuationToken = _continuationToken }, "");
                //var content = new StringContent(message, Encoding.UTF8, "application/json");
                var content = new FormUrlEncodedContent(new[]
                {
                    new KeyValuePair<string, string>("ContinuationToken", _continuationToken),
                });

                var response = await ((ConnectionDexih)ReferenceConnection).HttpPost("ReceiveData", content, false);

                if ((bool)response["success"])
                {
                    var receiveData = response["value"].ToObject<(bool IsComplete, DataModel Package)>();
                    _moreData = receiveData.IsComplete;

                    if (!_moreData && receiveData.Package == null)
                    {
                        _isOpen = false;
                        return null;
                    }
                    _columns = receiveData.Package.Columns;
                    _dataset = receiveData.Package.Data;

                    _columnOrdinals = new int[CacheTable.Columns.Count];
                    for(var i= 0; i< _columnOrdinals.Length; i++)
                    {
                        _columnOrdinals[i] = Array.IndexOf(_columns, CacheTable.Columns[i].Name);
                    }
                }
                else
                {
                    throw new ConnectionException($"Error {response?["message"]}", new Exception(response["exceptionDetails"].ToString()));
                }

                if(_dataset == null || !_dataset.Any()){
                    _isOpen = false;
                    return null;
                }
                else
                {
                    var row = ConvertRow( _dataset[0]);
                    _datasetRow = 1;
                    return row;
                }
            }
            catch (Exception ex)
            {
                throw new Exception("The hub reader service failed due to the following error: " + ex.Message, ex);
            }
        }

        private object[] ConvertRow(IReadOnlyList<object> row)
        {
            var newRow = new object[_columnOrdinals.Length];
            
            for(var i = 0; i < _columnOrdinals.Length; i++)
            {
                if(row[_columnOrdinals[i]] is JToken)
                {
                    newRow[i] = DBNull.Value;
                    continue;
                }

                switch(CacheTable.Columns[i].Datatype)
                {
                    case ETypeCode.Guid:
                        newRow[i] = Guid.Parse(row[_columnOrdinals[i]].ToString());
                        break;
                   default:
                       newRow[i] = row[_columnOrdinals[i]];
                       break;
                }

            }

            return newRow;

        }
        
        public class DataModel
        {
            public string[] Columns{ get; set; }
            public object[][] Data { get; set; }
        }


		public override bool CanLookupRowDirect { get; } = false;

        /// <summary>
        /// This performns a lookup directly against the underlying data source, returns the result, and adds the result to cache.
        /// </summary>
        /// <param name="filters"></param>
        /// <returns></returns>
        //public override async Task<ReturnValue<object[]>> LookupRowDirect(List<Filter> filters)
        //{
         //   return await ((ConnectionDexih) ReferenceConnection).LookupRow(CacheTable, filters);
         //}
    }
}
