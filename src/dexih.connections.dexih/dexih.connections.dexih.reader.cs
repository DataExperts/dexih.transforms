using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using static dexih.functions.DataType;
using System.Net.Http;
using Newtonsoft.Json.Linq;
using System.Threading;
using System.Text;

namespace dexih.connections.dexih
{
    public class ReaderDexih : Transform
    {
        private bool _isOpen = false;

		private int _datasetRow;
        private object[][] _dataset;
        private ERealTimeQueueStatus _datasetStatus;
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

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query, CancellationToken cancelToken)
        {
            AuditKey = auditKey;

            try
            {
                if (_isOpen)
                {
                    return new ReturnValue(false, "The information hub connection is already open.", null);
                }

				var message = Json.SerializeObject(new {HubName = ReferenceConnection.DefaultDatabase, SourceConnectionName = CacheTable.SourceConnectionName, TableName = CacheTable.Name, TableSchema = CacheTable.Schema, Query = query }, "");
				var content = new StringContent(message, Encoding.UTF8, "application/json");
				var response = await ((ConnectionDexih)ReferenceConnection).HttpPost("OpenTableQuery", content, true);

				if(!response.Success)
				{
					return response;
				}

                var returnMessage = Json.JTokenToObject<RemoteMessage>(response.Value, null);

                if(returnMessage.Success == false)
                {
                    return new ReturnValue(false, returnMessage.Message, returnMessage.Exception);
                }

				_continuationToken = response.Value["continuationToken"].ToString();
                ((ConnectionDexih)ReferenceConnection).SetContinuationToken(_continuationToken);
                CacheTable.ContinuationToken = _continuationToken;

				_datasetRow = 0;

                return new ReturnValue(true);
            }
            catch (Exception ex)
            {
                return new ReturnValue(false, "The following error occurred when starting the web service: " + ex.Message, ex);
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

        public override ReturnValue ResetTransform()
        {
            return new ReturnValue(true);
        }

        protected override async Task<ReturnValue<object[]>> ReadRecord(CancellationToken cancellationToken)
        {
            try
            {
				if (_dataset != null && _datasetRow < _dataset.Count())
				{
					var row = ConvertRow(_dataset[_datasetRow]);
                    _datasetRow++;

					return new ReturnValue<object[]>(true, row);
				}
				else if(!(_datasetStatus == ERealTimeQueueStatus.NotComplete))
				{
					_isOpen = false;
					return new ReturnValue<object[]>(false, null);
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

					if (!response.Success)
					{
						return new ReturnValue<object[]>(response);
					}

                    var popData = response.Value.ToObject<ReturnValue<RealTimeQueuePackage<object[][]>>>();

                    if(!popData.Success)
                    {
                        return new ReturnValue<object[]>(popData);
                    }
                    _datasetStatus = popData.Value.Status;
                    _dataset = popData.Value.Package;

					if(_dataset == null || _dataset.Count() == 0){
						_isOpen = false;
						return new ReturnValue<object[]>(false, null);
					}
					else
					{
						var row = ConvertRow( _dataset[0]);
						_datasetRow = 1;
						return new ReturnValue<object[]>(true, row);
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
