using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Mime;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Exceptions;
using dexih.transforms;
using Newtonsoft.Json.Linq;

namespace dexih.functions.external
{
    public class OpenWeatherMap: IDisposable
    {
        private const string apiKey = "API Key.  Sign up at [openweathermap.org](https://openweathermap.org/price).";
        private const string maxCalls = "Limit maximum api calls per minute (unlimited = 0)";

        private Stopwatch stopwatch;
        private int apiCallCount;
        
        public enum TemperatureScale
        {
            Fahrenheit = 1, Celsius, Kelvin
        }

        public class WeatherDetails
        {
            public double? Longitude { get; set; }
            public double? Latitude { get; set; }

            public string Weather { get; set; }
            public string Description { get; set; }

            public double? Temperature { get; set; }
            public double? Pressure { get; set; }
            public double? Humidity { get; set; }

            public double? TemperatureMin { get; set; }
            public double? TemperatureMax { get; set; }

            public int? Visibility { get; set; }

            public double? WindSpeed { get; set; }
            public int? WindDegrees { get; set; }

            public double? Cloudiness { get; set; }

            public double? Rain1Hour { get; set; }
            public double? Rain3Hour { get; set; }

            public double? Snow1Hour { get; set; }
            public double? Snow3Hour { get; set; }

            public DateTime? ReadingTime { get; set; }
            
            public string Country { get; set; }
            public DateTime? Sunrise { get; set; }
            public DateTime? Sunset { get; set; }

            public int CityId { get; set; }
            public string CityName { get; set; }
        }

        public class CityDetails
        {
            public int CityId { get; set; }
            public string CityName { get; set; }
            public string Country { get; set; }
            public double Longitude { get; set; }
            public double Latitude { get; set; }
        }

        // cache the httpClient.
        private HttpClient _httpClient;

        private string GetOpenWeatherUrl(string key) => $"http://api.openweathermap.org/data/2.5/weather?APPID={key}";

        private async Task<(string url, string statusCode, bool isSuccess, Stream response)> GetWebServiceResponse(
            string key, string parameters, CancellationToken cancellationToken)
        {
            var uri = new Uri(GetOpenWeatherUrl(key) + parameters);

            if (_httpClient == null)
            {
                _httpClient = new HttpClient()
                {
                    BaseAddress = new Uri(uri.GetLeftPart(UriPartial.Authority)),
                };

                _httpClient.DefaultRequestHeaders.Accept.Clear();
                _httpClient.DefaultRequestHeaders.CacheControl = new CacheControlHeaderValue()
                {
                    NoCache = true
                };
            }

            var response = await _httpClient.GetAsync(uri.PathAndQuery, cancellationToken);

            return (uri.ToString(), response.StatusCode.ToString(), response.IsSuccessStatusCode,
                await response.Content.ReadAsStreamAsync());
        }

        private double? ConvertTemperature(double? kelvin, TemperatureScale temperatureScale)
        {
            if (kelvin == null) return null;

            double temperature;
            switch (temperatureScale)
            {
                case TemperatureScale.Fahrenheit:
                    temperature = ((9.0 / 5.0) * (kelvin.Value - 273.15)) + 32;
                    break;
                case TemperatureScale.Celsius:
                    temperature =  kelvin.Value - 273.15;
                    break;
                case TemperatureScale.Kelvin:
                    temperature =  kelvin.Value;
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(temperatureScale), temperatureScale, null);
            }

            return Math.Round(temperature, 2);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Weather", Name = "Weather By City Name",
            Description = "Gets the weather based on a city name.")]
        public Task<WeatherDetails> WeatherByCityName(
            [TransformFunctionParameter(Description = apiKey)] string key, 
            string cityName,
            TemperatureScale temperatureScale, 
            [TransformFunctionParameter(Description = maxCalls)] int maxCallsMinute = 60, 
            CancellationToken cancellationToken = default)
        {
            return GetWeatherResponse(key, $"&q={cityName}", temperatureScale, maxCallsMinute, cancellationToken);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Weather", Name = "Weather By City Id",
            Description = "Gets the weather based on a city id.  Use the row function \"Weather Cities List\" for a list of cities.")]
        public Task<WeatherDetails> WeatherByCityId(
            [TransformFunctionParameter(Description = apiKey)] string key, 
            int cityId,
            TemperatureScale temperatureScale, 
            [TransformFunctionParameter(Description = maxCalls)] int maxCallsMinute = 60, 
            CancellationToken cancellationToken = default)
        {
            return GetWeatherResponse(key, $"&id={cityId}", temperatureScale, maxCallsMinute, cancellationToken);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Weather", Name = "Weather By Geo Coordinates",
            Description = "Gets the weather based on a longitude and latitude.")]
        public Task<WeatherDetails> WeatherByCoordinates(
            [TransformFunctionParameter(Description = apiKey)] string key, 
            double latitude, 
            double longitude,
            TemperatureScale temperatureScale, 
            [TransformFunctionParameter(Description = maxCalls)] int maxCallsMinute = 60, 
            CancellationToken cancellationToken = default)
        {
            return GetWeatherResponse(key, $"&lat={latitude}&lon={longitude}", temperatureScale, maxCallsMinute, cancellationToken);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Weather", Name = "Weather By Zip Code",
            Description = "Gets the weather based on a zip code.  Note if country is not specified when USA will be default.")]
        public Task<WeatherDetails> WeatherByZipCode(
            [TransformFunctionParameter(Description = apiKey)] string key, 
            string zipCode, 
            string country,
            TemperatureScale temperatureScale,
            [TransformFunctionParameter(Description = maxCalls)] int maxCallsMinute = 60, 
            CancellationToken cancellationToken = default)
        {
            return GetWeatherResponse(key, $"&zip={zipCode},{country}", temperatureScale, maxCallsMinute, cancellationToken);
        }


        private async Task<WeatherDetails> GetWeatherResponse(
            string key, 
            string uri, 
            TemperatureScale temperatureScale, 
            int maxCallsPerMinute,
            CancellationToken cancellationToken)
        {
            if (stopwatch == null && maxCallsPerMinute > 0)
            {
                stopwatch = Stopwatch.StartNew();
                apiCallCount = 0;
            }

            if (stopwatch != null && apiCallCount >= maxCallsPerMinute)
            {
                if (stopwatch.Elapsed.Seconds < 60)
                {
                    await Task.Delay(TimeSpan.FromMinutes(1) - stopwatch.Elapsed, cancellationToken);
                }
                
                stopwatch.Reset();
                apiCallCount = 0;
            }

            apiCallCount++;
            
            var (url, statusCode, isSuccess, response) = await GetWebServiceResponse(key, uri, cancellationToken);

            if (!isSuccess)
            {
                throw new FunctionException(
                    $"Could not receive weather data due to failure to connect to url ({url}).  The response status was {statusCode}.");
            }

            var weatherDetails = new WeatherDetails();

            var reader = new StreamReader(response);
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

            var coord = jToken["coord"];
            weatherDetails.Latitude = coord["lat"]?.Value<double>();
            weatherDetails.Longitude = coord["lon"]?.Value<double>();

            var weather = jToken["weather"][0];
            weatherDetails.Weather = weather["main"]?.Value<string>();
            weatherDetails.Description = weather["description"]?.Value<string>();

            var main = jToken["main"];
            weatherDetails.Temperature = ConvertTemperature(main["temp"]?.Value<double?>(), temperatureScale);
            weatherDetails.Pressure = main["pressure"]?.Value<double?>();
            weatherDetails.Humidity = main["humidity"]?.Value<double?>();
            weatherDetails.TemperatureMin = ConvertTemperature(main["temp_min"]?.Value<double?>(), temperatureScale);
            weatherDetails.TemperatureMax = ConvertTemperature(main["temp_max"]?.Value<double?>(), temperatureScale);

            weatherDetails.Visibility = jToken["visibility"]?.Value<int?>();

            var wind = jToken["wind"];
            if (wind != null)
            {
                weatherDetails.WindSpeed = wind["speed"]?.Value<double?>();
                weatherDetails.WindDegrees = wind["deg"]?.Value<int?>();
            }

            var clouds = jToken["clouds"];
            if (clouds != null)
            {
                weatherDetails.Cloudiness = clouds["all"]?.Value<int?>();
            }

            var rain = jToken["rain"];
            if (rain != null)
            {
                weatherDetails.Rain1Hour = rain["1hr"]?.Value<double?>();
                weatherDetails.Rain3Hour = rain["3hr"]?.Value<double?>();
            }

            var snow = jToken["snow"];
            if (snow != null)
            {
                weatherDetails.Snow1Hour = snow["1hr"]?.Value<double?>();
                weatherDetails.Snow3Hour = snow["3hr"]?.Value<double?>();
            }

            weatherDetails.ReadingTime = jToken["dt"]?.Value<long>().UnixTimeStampToDate();

            var sys = jToken["sys"];
            if (sys != null)
            {
                weatherDetails.Country = sys["country"]?.Value<string>();
                weatherDetails.Sunrise = sys["sunrise"]?.Value<long>().UnixTimeStampToDate();
                weatherDetails.Sunset = sys["sunset"]?.Value<long>().UnixTimeStampToDate();
            }

            weatherDetails.CityId = jToken["id"].Value<int>();
            weatherDetails.CityName = jToken["name"].Value<string>();

            return weatherDetails;
        }

        private JArray _cachedCities;
        private int _index;
        private int _childCount;
        
        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Weather", Name = "Weather City List",
            Description = "Get a list of all the cities used for the weather functions.  Data from [openweathermap.org](https://openweathermap.org)")]
        public async Task<CityDetails> WeatherCities(CancellationToken cancellationToken)
        {
            if (_cachedCities == null)
            {
                var uri = new Uri("http://bulk.openweathermap.org/sample/city.list.min.json.gz");
                string jsonString;
;
                using (var client = new HttpClient())
                {
                    client.BaseAddress = new Uri(uri.GetLeftPart(UriPartial.Authority));
                    client.DefaultRequestHeaders.Accept.Clear();

                    var response = await client.GetAsync(uri.PathAndQuery, cancellationToken);

                    if (response.IsSuccessStatusCode)
                    {
                        using( var stream = await response.Content.ReadAsStreamAsync())
                        using( var output = new MemoryStream())
                        using( var sr = new GZipStream(stream, CompressionMode.Decompress))
                        {
                            await sr.CopyToAsync(output);
                            jsonString = Encoding.UTF8.GetString(output.GetBuffer(), 0, (int) output.Length);
                        }
                    }
                    else
                    {
                        return null;
                    }
                }
                
                try
                {
                    var cities = JToken.Parse(jsonString);
                    if (cities == null)
                    {
                        throw new FileHandlerException("The json data parsing returned nothing.");
                    }
                    
                    if (cities is JArray jArray)
                    {
                        _cachedCities = jArray;
                        _index = 0;
                        _childCount = _cachedCities.Children().Count();
                    }
                    else
                    {
                        throw new FileHandlerException($"The json data read, as a json array was expected.");
                    }
                }
                catch (Exception ex)
                {
                    throw new FileHandlerException($"The json data could not be parsed.  {ex.Message}", ex);
                }
            }

            if (_index < _childCount)
            {    
                var token = _cachedCities[_index];
                var city = new CityDetails()
                {
                    CityId = token["id"].Value<int>(),
                    CityName = token["name"].Value<string>(),
                    Country = token["country"].Value<string>(),
                    Longitude = token["coord"]?["lon"].Value<double>() ?? 0,
                    Latitude = token["coord"]?["lat"].Value<double>() ?? 0,
                };
                _index++;
                return city;
            }

            _cachedCities = null;

            return null;
        }

        public void Dispose()
        {
            _httpClient?.Dispose();
        }
    }
}