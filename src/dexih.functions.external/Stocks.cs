using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Exceptions;
using dexih.transforms;


namespace dexih.functions.external
{
    public class Stocks
    {
        private const string KeyDescription =
            "Sign up for key at [alphavantage.co](https://www.alphavantage.co/support/#api-key).";
        public struct StockEntity
        {
            public string Symbol { get; set; }
            public DateTime Time { get; set; }
            public double Open { get; set; }
            public double High { get; set; }
            public double Low { get; set; }
            public double Close { get; set; }
            public long Volume { get; set; }
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
        
      private async Task<List<StockEntity>> GetStockResponse(string uri, int maxCount, CancellationToken cancellationToken)
        {
            var response = await GetWebServiceResponse(uri, cancellationToken);

            var stockEntities = new List<StockEntity>();

            if (response.isSuccess)
            {
                var reader = new StreamReader(response.response);
                // var jsonString = await reader.ReadToEndAsync();
            
                JsonDocument jsonDocument;

                try
                {
                    jsonDocument = await JsonDocument.ParseAsync(response.response, cancellationToken: cancellationToken);
                    if (jsonDocument == null)
                    {
                        throw new FileHandlerException("The json data parsing returned nothing.");
                    }
                }
                catch (Exception ex)
                {
                    throw new FileHandlerException($"The json data could not be parsed.  {ex.Message}", ex);
                }

                if (jsonDocument.RootElement.TryGetProperty("Error Message", out var errorMessage))
                {
                    throw new FunctionException($"The stock service returned an error: {errorMessage.GetString()}");
                }

                
                if (!jsonDocument.RootElement.TryGetProperty("Meta Data", out var metadata))
                {
                    throw new FunctionException("The stock data from AlphaVantage didn't contain the metadata elements.");
                }

                var rootArray = jsonDocument.RootElement.EnumerateObject().ToArray();

                if (rootArray.Length < 2)
                {
                    throw new FunctionException("The stock data from AlphaVantage didn't contain the metadata and time series elements.");
                }

                var timeSeries = rootArray[1].Value;

                var symbol = metadata.GetProperty("2. Symbol").GetString();
                
                var count = 0;
                foreach (var stockTime in timeSeries.EnumerateObject())
                {
                    var stockEntity = new StockEntity
                    {
                        Symbol = symbol,
                        Time = Convert.ToDateTime(stockTime.Name),
                        Open = Convert.ToDouble(stockTime.Value.GetProperty("1. open").GetString()),
                        High = Convert.ToDouble(stockTime.Value.GetProperty("2. high").GetString()),
                        Low = Convert.ToDouble(stockTime.Value.GetProperty("3. low").GetString()),
                        Close = Convert.ToDouble(stockTime.Value.GetProperty("4. close").GetString()),
                        Volume = Convert.ToInt64(stockTime.Value.GetProperty("5. volume").GetString())
                    };
                            
                    stockEntities.Add(stockEntity);

                    count++;
                    if (count > maxCount) return stockEntities;


                }
            }
            else
            {
                throw new FunctionException($"Could not receive stock data due to failure to connect to url ({uri}).  The response status was {response.statusCode}.");
            }

            return stockEntities;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Stocks", Name = "Latest Stock Information",
            Description ="Gets the latest time interval data for the specified stock.")]
        public async Task<StockEntity> LatestStockInfo(
            [TransformFunctionParameter(Description = KeyDescription), TransformFunctionPassword] string key, 
            string symbol,
            [TransformFunctionParameter(Name = "Interval", Description = "Interval between quotes", ListOfValues = new[] {"1min", "5min", "15min", "30min", "60min"} )] string interval = "1min",
            CancellationToken cancellationToken = default)
        {
            var url = GetAlphaVantageUrl("TIME_SERIES_INTRADAY", interval, symbol, key);
            var entities = await GetStockResponse(url, 1, cancellationToken);

            if (entities.Count > 0)
            {
                return entities[0];
            }
            
            throw new FunctionException("The stock data from AlphaVantage could not be retrieved.");
        }
        
        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Stocks", Name = "Latest Stock History",
            Description =
                "Gets the latest stock history time interval data for the specified stock.")]
        public Task<List<StockEntity>> LatestStockHistory(
            [TransformFunctionParameter(Description = KeyDescription), TransformFunctionPassword] string key, 
            string symbol,
            int maxCount = 1,
            [TransformFunctionParameter(Name = "Interval", Description = "Interval between quotes", ListOfValues = new[] {"1min", "5min", "15min", "30min", "60min"} )] string interval = "15min",
            CancellationToken cancellationToken = default)
        {
            var url = GetAlphaVantageUrl("TIME_SERIES_INTRADAY", interval, symbol, key);
            return GetStockResponse(url, maxCount, cancellationToken);
        }
    }
}