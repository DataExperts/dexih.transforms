using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.XPath;
using dexih.functions.Exceptions;
using dexih.transforms;
using Newtonsoft.Json.Linq;

namespace dexih.functions.external
{
    public class Currency
    {
        private const string KeyAttribute =
            "Register for an API key at: [currencylayer.com](https://currencylayer.com/product).";
        public class CurrencyDetails
        {
           
            public DateTime TimeStamp { get; set; }
            public DateTime Date { get; set; }
            public string Source { get; set; }

            public Dictionary<string, double> Rates = new Dictionary<string, double>();

            public double USDRate(string to)
            {
                if (to == "USD")
                {
                    return 1;
                }
                else
                {
                    if (string.IsNullOrEmpty(to))
                    {
                        throw new FunctionException($"The \"TO\" currency code was empty or null.");
                    }
                    if (!Rates.TryGetValue(to, out var rate))
                    {
                        throw new FunctionException($"The exchange rate for {to} is not available.");
                    }
                    
                    if (Math.Abs(rate) < 0.0000001)
                    {
                        throw new ArgumentOutOfRangeException($"The rate for {to} is equal to 0.");
                    }

                    return rate;
                }

            }

            // get the from/to usd rates and divide these to get the from/to rate.
            public double Rate(string from, string to)
            {
                var rate1 = USDRate(from);
                var rate2 = USDRate(to);

                return rate2 / rate1;
            }
        }

        public class CurrencyRate
        {
            public string Currency { get; set; }
            public double ToRate { get; set; }
            public double FromRate { get; set; }
        }

        public class CountryCurrencyEntity
        {
            public string Country { get; set; }
            public string CurrencyName { get; set; }
            public string CurrencyCode { get; set; }
            public int CurrencyNumber { get; set; }
        }
        
        public class CurrencyEntity
        {
            public string CurrencyName { get; set; }
            public string CurrencyCode { get; set; }
            public int CurrencyNumber { get; set; }
        }

        private CurrencyDetails _liveCurrency;
        private Dictionary<string, CurrencyDetails> _currencyHistory;
        
        private double _baseRate;
        private Dictionary<string, double>.Enumerator _ratesEnumerator;
        
        private bool _isFirst = true;
        
        private async Task<(string url, string statusCode, bool isSuccess, Stream response)> GetWebServiceResponse(string url, CancellationToken cancellationToken)
        {
            var uri = new Uri(url);
            
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(uri.GetLeftPart(UriPartial.Authority));
                client.DefaultRequestHeaders.Accept.Clear();

                var response = await client.GetAsync(uri.PathAndQuery, cancellationToken);

                return (uri.ToString(), response.StatusCode.ToString(), response.IsSuccessStatusCode, await response.Content.ReadAsStreamAsync());
            }
        }

        private async Task LoadLiveRates(string key, CancellationToken cancellationToken)
        {
            _liveCurrency = await GetRates($"http://apilayer.net/api/live?access_key={key}", cancellationToken);
        }
        
        private async Task<CurrencyDetails> GetRates(string url, CancellationToken cancellationToken)
        {
            var response = await GetWebServiceResponse(url, cancellationToken);

            if (!response.isSuccess)
            {
                throw new FunctionException(
                    $"The currency data not loaded due to failure to contact apilayer.net server.  Status code: {response.statusCode}.");
            }

            var reader = new StreamReader(response.response);
            var jsonString = await reader.ReadToEndAsync();

            JToken jToken;

            try
            {
                jToken = JToken.Parse(jsonString);
                if (jToken == null)
                {
                    throw new FileHandlerException(
                        "The currency data not available, json parsing returned nothing.");
                }
            }
            catch (Exception ex)
            {
                throw new FileHandlerException(
                    $"The currency data not available, error returned parsing json data: {ex.Message}", ex);
            }

            var success = jToken["success"].Value<bool>();

            if (!success)
            {
                var error = jToken["error"];
                if (error != null)
                {
                    var errorMessage = error["info"].Value<string>();
                    throw new FunctionException($"The currency data not loaded, error message: {errorMessage}.");
                }
                else
                {
                    throw new FunctionException($"The currency data not loaded due to an unknown error.");
                }

            }

            var rates = new CurrencyDetails();

            rates.TimeStamp = jToken["timestamp"].Value<long>().UnixTimeStampToDate();
            rates.Source = jToken["source"].Value<string>();

            var quotes = jToken["quotes"];
            if (quotes != null)
            {
                foreach (var quote in quotes.Children())
                {
                    if (quote is JProperty property)
                    {
                        var sourceTo = property.Name; // should contain string like USDAUD
                        var toCurrency = sourceTo.Substring(3, 3);
                        var rate = property.Value.Value<double>();
                        rates.Rates.Add(toCurrency, rate);
                    }
                }
            }

            return rates;
        }

        private async Task<CurrencyDetails> GetHistoricalRates(string key, DateTime dateTime, CancellationToken cancellationToken)
        {
            if (_isFirst)
            {
                _isFirst = false;
                _currencyHistory = new Dictionary<string, CurrencyDetails>();
            }

            var date = dateTime.ToString("yyyy-MM-dd");

            if (_currencyHistory.TryGetValue(date, out var currencyDetails))
            {
                return currencyDetails;
            }

            var url = $"http://apilayer.net/api/historical?access_key={key}&date={date}";
            var rates = await GetRates(url, cancellationToken);
            _currencyHistory.Add(date, rates);

            return rates;
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Currency", Name = "Live Exchange Rate",
            Description = "Get the current exchange rate for the from/to currency.")]
        public async Task<double> CurrencyRateLive(
            [TransformFunctionParameter(Description = KeyAttribute)] string key, 
            string from, 
            string to, 
            CancellationToken cancellationToken)
        {
            if (from == to) return 1;
            
            // load the rates on the first call
            if (_isFirst)
            {
                _isFirst = false;
                await LoadLiveRates(key, cancellationToken);
            }

            if (_liveCurrency == null)
            {
                throw new FunctionException($"The currency data not loaded.");
            }


            return _liveCurrency.Rate(from, to);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Currency", Name = "Live Rate Convert",
            Description = "Converts the value based on the current exchange rate for the from/to currency.")]
        public async Task<double> CurrencyConvertLive(
            [TransformFunctionParameter(Description = KeyAttribute)] string key, 
            double value, 
            string from, 
            string to, 
            CancellationToken cancellationToken)
        {
            if (from == to) return value;
            
            // load the rates on the first call
            if (_isFirst)
            {
                _isFirst = false;
                await LoadLiveRates(key, cancellationToken);
            }

            if (_liveCurrency == null)
            {
                throw new FunctionException($"The currency data not loaded.");
            }

            return _liveCurrency.Rate(from, to) * value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Currency", Name = "Live Exchange Rates",
            Description = "Gets the live exchange rates for a specified currency (if from is null USD is used).")]
        public async Task<CurrencyRate> CurrenciesLive(
            [TransformFunctionParameter(Description = KeyAttribute)] string key, 
            string from, 
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(from)) from = "USD";
            
            // load the rates on the first call
            if (_isFirst)
            {
                _isFirst = false;
                await LoadLiveRates(key, cancellationToken);

                _baseRate = _liveCurrency.USDRate(from);
                _ratesEnumerator = _liveCurrency.Rates.GetEnumerator();
            }
            
            if (_liveCurrency == null)
            {
                throw new FunctionException($"The currency data not loaded.");
            }

            if (_ratesEnumerator.MoveNext())
            {

                var currency = new CurrencyRate()
                {

                    Currency = _ratesEnumerator.Current.Key,
                    ToRate = _ratesEnumerator.Current.Value / _baseRate,
                    FromRate = _baseRate / _ratesEnumerator.Current.Value
                };

                return currency;
            }
            else
            {
                _ratesEnumerator.Dispose();
                return null;
            }
        }
        
      [TransformFunction(FunctionType = EFunctionType.Map, Category = "Currency", Name = "Historical Exchange Rate",
            Description = "Get the exchange rate at the specified date for the from/to currency.")]
        public async Task<double> CurrencyRateHistorical(
            [TransformFunctionParameter(Description = KeyAttribute)] string key, 
            DateTime date, 
            string from, 
            string to, 
            CancellationToken cancellationToken)
        {
            if (from == to) return 1;

            var rates = await GetHistoricalRates(key, date, cancellationToken);
            return rates.Rate(from, to);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Currency", Name = "Historical Rate Convert",
            Description = "Converts the value based on the current exchange rate for the from/to currency.")]
        public async Task<double> CurrencyConvertHistorical(
            [TransformFunctionParameter(Description = KeyAttribute)] string key, 
            DateTime date, 
            double value, 
            string from, 
            string to, 
            CancellationToken cancellationToken)
        {
            if (from == to) return value;
            
            var rates = await GetHistoricalRates(key, date, cancellationToken);

            return rates.Rate(from, to) * value;
        }
        
        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Currency", Name = "Historical Exchange Rates",
            Description = "Gets the live exchange rates for a specified currency (if from is null USD is used).")]
        public async Task<CurrencyRate> CurrenciesHistorical(
            [TransformFunctionParameter(Description = KeyAttribute)] string key, 
            DateTime date, 
            string from, 
            CancellationToken cancellationToken)
        {
            if (string.IsNullOrEmpty(from)) from = "USD";
            
            // load the rates on the first call
            if (_isFirst)
            {
                var rates = await GetHistoricalRates(key, date, cancellationToken);
                _baseRate = rates.USDRate(from);
                _ratesEnumerator = rates.Rates.GetEnumerator();
            }
            
            if (_ratesEnumerator.MoveNext())
            {
                var currency = new CurrencyRate()
                {
                    Currency = _ratesEnumerator.Current.Key,
                    ToRate = _ratesEnumerator.Current.Value / _baseRate,
                    FromRate = _baseRate / _ratesEnumerator.Current.Value
                };

                return currency;
            }
            else
            {
                _ratesEnumerator.Dispose();
                return null;
            }
        }

        private XPathNodeIterator _cachedCurrencyCodes;
        private HashSet<string> _usedCurrencyCodes;

        private async Task<bool> LoadCurrencyCode(CancellationToken cancellationToken)
        {
            var uri = new Uri("https://www.currency-iso.org/dam/downloads/lists/list_one.xml");
            
            using (var client = new HttpClient())
            {
                client.BaseAddress = new Uri(uri.GetLeftPart(UriPartial.Authority));
                client.DefaultRequestHeaders.Accept.Clear();

                var response = await client.GetAsync(uri.PathAndQuery, cancellationToken);

                if (response.IsSuccessStatusCode)
                {
                    try
                    {
                        var xPathDocument = new XPathDocument(await response.Content.ReadAsStreamAsync());
                        var xPathNavigator = xPathDocument.CreateNavigator();

                        if (xPathNavigator == null)
                        {
                            throw new FileHandlerException($"Failed to parse the response xml value.");
                        }

                        _cachedCurrencyCodes = xPathNavigator.Select("//CcyTbl/*");
                        return true;
                    }
                    catch (Exception ex)
                    {
                        throw new FileHandlerException($"Failed to parse the response xml value. {ex.Message}", ex);
                    }
                }
                else
                {
                    return false;
                }
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Currency", Name = "Currency name and codes.",
            Description = "Distinct list of currency, fund and precious metal codes. Data is standard ISO 4217 [currency-iso.org](https://www.currency-iso.org/en/home/tables/table-a1.html).")]
        public async Task<CurrencyEntity> CurrencyCodes(CancellationToken cancellationToken)

        {
            if (_cachedCurrencyCodes == null)
            {
                if (!await LoadCurrencyCode(cancellationToken))
                {
                    return null;
                }
                
                _usedCurrencyCodes = new HashSet<string>();
            }

            if (_cachedCurrencyCodes == null) return null;

            while (_cachedCurrencyCodes.MoveNext())
            {
                var node = _cachedCurrencyCodes.Current;

                var currencyCode = node.SelectSingleNode("Ccy")?.Value;

                if (_usedCurrencyCodes.Contains(currencyCode)) continue;

                _usedCurrencyCodes.Add(currencyCode);

                var currencyEntity = new CurrencyEntity()
                {
                    CurrencyNumber = node.SelectSingleNode("CcyNbr")?.ValueAsInt ?? 0,
                    CurrencyCode = currencyCode,
                    CurrencyName = node.SelectSingleNode("CcyNm")?.Value,
                };

                return currencyEntity;
            }

            return null;

        }
        

        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Currency", Name = "Country to currency name and codes",
            Description = "List of currency, fund and precious metal codes by country (currency codes are not unique in this list).  Data is standard ISO 4217 from [currency-iso.org](https://www.currency-iso.org/en/home/tables/table-a1.html).")]
        public async Task<CountryCurrencyEntity> CountryCurrencyCodes(CancellationToken cancellationToken)
        {
            if (_cachedCurrencyCodes == null)
            {
                if (!await LoadCurrencyCode(cancellationToken))
                {
                    return null;
                }
            }

            if (_cachedCurrencyCodes == null) return null;
            if(!_cachedCurrencyCodes.MoveNext()) return null;
            var node = _cachedCurrencyCodes.Current;

            var currencyEntity = new CountryCurrencyEntity()
            {
                Country = node.SelectSingleNode("CtryNm")?.Value,
                CurrencyNumber = node.SelectSingleNode("CcyNbr")?.ValueAsInt ?? 0,
                CurrencyCode = node.SelectSingleNode("Ccy")?.Value,
                CurrencyName = node.SelectSingleNode("CcyNm")?.Value,
            };

            return currencyEntity;
     
        }
        
    }
}