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

		private JEnumerable<JToken> dataset;
		private int datasetRow;
		private bool datasetComplete;
		private string continuationToken;


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

        public override async Task<ReturnValue> Open(Int64 auditKey, SelectQuery query)
        {
            AuditKey = auditKey;

            try
            {
                if (_isOpen)
                {
                    return new ReturnValue(false, "The web service connection is already open.", null);
                }

				var message = Json.SerializeObject(new { TableName = CacheTable.TableName, Query = query }, "");
				var content = new StringContent(message, Encoding.UTF8, "application/json");
				var response = await ((ConnectionDexih)ReferenceConnection).HttpPost("GetTableData", content);

				if(!response.Success)
				{
					return response;
				}
				continuationToken = response.Value["continuationToken"].ToString();
				datasetComplete = (bool)response.Value["datasetComplete"];
				dataset = response.Value["dataset"].Children();
				datasetRow = 0;

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
				if (datasetRow < dataset.Count())
				{
					var result = dataset[datasetRow].ToArray<object>();
					datasetRow++;
					return new ReturnValue<object[]>(true, result);
				}
				else if(datasetComplete)
				{
					_isOpen = false;
					return new ReturnValue<object[]>(false, null);
				}
				else
				{
					var message = Json.SerializeObject(new { ContinuationToken = continuationToken }, "");
					var content = new StringContent(message, Encoding.UTF8, "application/json");
					var response = await ((ConnectionDexih)ReferenceConnection).HttpPost("GetTableData", content);

					if (!response.Success)
					{
						return new ReturnValue<object[]>(response);
					}
					continuationToken = response.Value["continuationToken"].ToString();
					datasetComplete = (bool)response.Value["datasetComplete"];
					dataset = response.Value["dataset"].Children();

					if(dataset.Count() == 0){
						_isOpen = false;
						return new ReturnValue<object[]>(false, null);
					}
					else
					{
						var result = dataset[0].ToArray<object>();
						datasetRow = 1;
						return new ReturnValue<object[]>(true, result);
					}
				}
            }
            catch (Exception ex)
            {
                throw new Exception("The hub reader service failed due to the following error: " + ex.Message, ex);
            }
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
