using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Exceptions;
using dexih.transforms;
using Newtonsoft.Json.Linq;

namespace dexih.functions.external
{
    public class Stocks
    {
        public class StockEntity
        {
            public string Symbol { get; set; }
            public DateTime Time { get; set; }
            public double Open { get; set; }
            public double High { get; set; }
            public double Low { get; set; }
            public double Close { get; set; }
            public long Volume { get; set; }

            public string ResponseStatusCode { get; set; }
            public bool ResponseSuccess { get; set; }
        }

        private string GetAlphaVantageUrl(string function, string interval, string symbol, string key)
        {
            return
                $"https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey={key}";
        }
        
        private async Task<(string url, string statusCode, bool isSuccess, Stream response)> GetWebServiceResponse(
            string url, CancellationToken cancellationToken)
        {
            var uri = new Uri(url);

            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(uri.GetLeftPart(UriPartial.Authority));
                client.DefaultRequestHeaders.Accept.Clear();

                var response = await client.GetAsync(uri.PathAndQuery, cancellationToken);

                return (uri.ToString(), response.StatusCode.ToString(), response.IsSuccessStatusCode,
                    await response.Content.ReadAsStreamAsync());
            }
        }
        
      private async Task<StockEntity[]> GetStockResponse(string uri, int maxCount = Int32.MaxValue)
        {
            var response = await GetWebServiceResponse(uri, CancellationToken.None);

            var stockEntities = new List<StockEntity>();

            if (response.isSuccess)
            {
                var reader = new StreamReader(response.response);
                var jsonString = await reader.ReadToEndAsync();
            
                JToken jToken;

                try
                {
                    jToken = JToken.Parse(jsonString);
                    if (jToken == null)
                    {
                        throw new FileHandlerException("The json data parsing returned nothing.");
                    }
                }
                catch (Exception ex)
                {
                    throw new FileHandlerException($"The json data could not be parsed.  {ex.Message}", ex);
                }

                var errorMessage = jToken["Error Message"];
                if (errorMessage != null)
                {
                    throw new FunctionException($"The stock service returned an error: {errorMessage.Value<string>()}");
                }

                var metadata = jToken["Meta Data"];
                var timeSeries = jToken.Children().ElementAt(1);

                if (metadata == null || timeSeries == null)
                {
                    throw new FunctionException("The stock data from AlphaVantage didn't contain the metadata and time series elements.");
                }

                var count = 0;
                foreach (JToken stock in timeSeries.Children())
                {
                    var stockEntity = new StockEntity
                    {
                        ResponseSuccess = response.isSuccess,
                        ResponseStatusCode = response.statusCode,
                        Symbol = metadata["2. Symbol"].Value<string>()
                    };

                    var entity = stock.Children().First();

                    if (entity is JProperty property)
                    {
                        stockEntity.Time = Convert.ToDateTime(property.Name);
                        var values = property.Children();
                        stockEntity.Open = values["1. open"].ElementAt(0).Value<double>();
                        stockEntity.High = values["2. high"].ElementAt(0).Value<double>();
                        stockEntity.Low = values["3. low"].ElementAt(0).Value<double>();
                        stockEntity.Close = values["4. close"].ElementAt(0).Value<double>();
                        stockEntity.Volume = values["5. volume"].ElementAt(0).Value<long>();
                    }

                    stockEntities.Add(stockEntity);

                    count++;
                    if (count > maxCount) break;
                }
            }
            else
            {
                var stockEntity = new StockEntity
                {
                    ResponseSuccess = response.isSuccess,
                    ResponseStatusCode = response.statusCode,
                };
                stockEntities.Add(stockEntity);
            }

            return stockEntities.ToArray();
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Stocks", Name = "Latest Stock Information",
            Description =
                "Gets the latest time interval data for the specified stock.  Sign up for key at [alphavantage.co](https://www.alphavantage.co/support/#api-key).")]
        public async Task<StockEntity> LatestStockInfo(
            string key, 
            string symbol,
            [TransformFunctionParameter(Name = "Interval", Description = "Interval between quotes", ListOfValues = new[] {"1min", "5min", "15min", "30min", "60min"} )] string interval )
        {
            var url = GetAlphaVantageUrl("TIME_SERIES_INTRADAY", interval, symbol, key);
            var entities = await GetStockResponse(url, 1);

            if (entities.Length > 0)
            {
                return entities[0];
            }
            
            throw new FunctionException("The stock data from AlphaVantage could not be retrieved.");
        }
        
//        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Stocks", Name = "Latest Stock History",
//            Description =
//                "Gets the latest stock history time interval data for the specified stock.  Sign up for key at [alphavantage.co](https://www.alphavantage.co/support/#api-key).")]
//        public Task<StockEntity[]> LatestStockHistory(
//            string key, 
//            string symbol,
//            [TransformFunctionParameter(Name = "Interval", Description = "Interval between quotes", ListOfValues = new[] {"1min", "5min", "15min", "30min", "60min"} )] string interval )
//        {
//            var url = GetAlphaVantageUrl("TIME_SERIES_INTRADAY", interval, symbol, key);
//            return GetStockResponse(url);
//        }
    }
}