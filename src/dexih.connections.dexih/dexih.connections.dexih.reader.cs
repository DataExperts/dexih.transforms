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
using Dexih.Utils.RealTimeBuffer;
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
        private object[][] _dataset;
        private ERealTimeBufferStatus _datasetStatus;
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

        public override async Task<bool> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
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
                    ((ConnectionDexih)ReferenceConnection).SetContinuationToken(_continuationToken);
                    CacheTable.ContinuationToken = _continuationToken;
                }
                else
                {
                    throw new ConnectionException($"Error {response?["message"]}", new Exception(response["exceptionDetails"].ToString()));
                }

				_datasetRow = 0;

                return true;
            }
            catch (Exception ex)
            {
                throw new ConnectionException($"Opening connection to information hub failed.  {ex.Message}");
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
				if (_dataset != null && _datasetRow < _dataset.Count())
				{
					var row = ConvertRow(_dataset[_datasetRow]);
                    _datasetRow++;

                    return row;
				}
				else if(!(_datasetStatus == ERealTimeBufferStatus.NotComplete))
				{
					_isOpen = false;
                    return null;
				}
				else
				{
                    //var message = Json.SerializeObject(new { ContinuationToken = _continuationToken }, "");
                    //var content = new StringContent(message, Encoding.UTF8, "application/json");
                    var content = new FormUrlEncodedContent(new[]
                    {
                        new KeyValuePair<string, string>("ContinuationToken", _continuationToken),
                    });

                    var response = await ((ConnectionDexih)ReferenceConnection).HttpPost("PopData", content, false);

                    if ((bool)response["success"])
                    {
                        var popData = response["value"].ToObject<RealTimeBufferPackage<object[][]>>();
                        _datasetStatus = popData.Status;
                        _dataset = popData.Package;
                    }
                    else
                    {
                        throw new ConnectionException($"Error {response?["message"]}", new Exception(response["exceptionDetails"].ToString()));
                    }

					if(_dataset == null || _dataset.Count() == 0){
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
            }
            catch (Exception ex)
            {
                throw new Exception("The hub reader service failed due to the following error: " + ex.Message, ex);
            }
        }

        private object[] ConvertRow(object[] row)
        {
            for(int i = 0; i < row.Length; i++)
            {
                switch(CacheTable.Columns[i].Datatype)
                {
                    case ETypeCode.Guid:
                        row[i] = Guid.Parse(row[i].ToString());
                        break;
                }

                if(row[i] is JToken)
                {
                    row[i] = DBNull.Value;
                }
            }

            return row;

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
