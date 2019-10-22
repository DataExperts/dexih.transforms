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
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Exceptions;
using dexih.transforms;


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
            [TransformFunctionParameter(Description = apiKey), TransformFunctionPassword] string key, 
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
            [TransformFunctionParameter(Description = apiKey), TransformFunctionPassword] string key, 
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
            [TransformFunctionParameter(Description = apiKey), TransformFunctionPassword] string key, 
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
            [TransformFunctionParameter(Description = apiKey), TransformFunctionPassword] string key, 
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
                try
                {
                    var errorResponse = await JsonDocument.ParseAsync(response, cancellationToken: cancellationToken);
                    var message = errorResponse.RootElement.GetProperty("message").ToString();
                    throw new FunctionException("Could not receive weather data: " + message);
                }
                catch (FunctionException)
                {
                    throw;
                }
                catch
                {
                    throw new FunctionException(
                        $"Could not receive weather data due to failure to connect to url ({url}).  The response status was {statusCode}.");
                }
            }

            var weatherDetails = new WeatherDetails();

            var reader = new StreamReader(response);
            var jsonString = await reader.ReadToEndAsync();

            JsonDocument jsonDocument;

            try
            {
                jsonDocument = JsonDocument.Parse(jsonString);
                if (jsonDocument == null)
                {
                    throw new FileHandlerException("The json data parsing returned nothing.");
                }
            }
            catch (Exception ex)
            {
                throw new FileHandlerException($"The json data could not be parsed.  {ex.Message}", ex);
            }

            var coord = jsonDocument.RootElement.GetProperty("coord");
            weatherDetails.Latitude = coord.GetProperty("lat").GetDouble();
            weatherDetails.Longitude = coord.GetProperty("lon").GetDouble();

            var weather = jsonDocument.RootElement.GetProperty("weather")[0];
            weatherDetails.Weather = weather.GetProperty("main").GetString();
            weatherDetails.Description = weather.GetProperty("description").GetString();

            var main = jsonDocument.RootElement.GetProperty("main");
            weatherDetails.Temperature = ConvertTemperature(main.GetProperty("temp").GetDouble(), temperatureScale);
            weatherDetails.Pressure = main.GetProperty("pressure").GetDouble();
            weatherDetails.Humidity = main.GetProperty("humidity").GetDouble();
            weatherDetails.TemperatureMin = ConvertTemperature(main.GetProperty("temp_min").GetDouble(), temperatureScale);
            weatherDetails.TemperatureMax = ConvertTemperature(main.GetProperty("temp_max").GetDouble(), temperatureScale);

            weatherDetails.Visibility = jsonDocument.RootElement.GetProperty("visibility").GetInt32();

            if (jsonDocument.RootElement.TryGetProperty("wind", out var wind))
            {
                weatherDetails.WindSpeed = wind.GetProperty("speed").GetDouble();
                weatherDetails.WindDegrees = wind.GetProperty("deg").GetInt32();
            }

            if (jsonDocument.RootElement.TryGetProperty("clouds", out var clouds))
            {
                weatherDetails.Cloudiness = clouds.GetProperty("all").GetInt32();
            }

            if (jsonDocument.RootElement.TryGetProperty("rain", out var rain))
            {
                weatherDetails.Rain1Hour = rain.GetProperty("1hr").GetDouble();
                weatherDetails.Rain3Hour = rain.GetProperty("3hr").GetDouble();
            }

            if (jsonDocument.RootElement.TryGetProperty("snow", out var snow))
            {
                weatherDetails.Snow1Hour = snow.GetProperty("1hr").GetDouble();
                weatherDetails.Snow3Hour = snow.GetProperty("3hr").GetDouble();
            }

            weatherDetails.ReadingTime = jsonDocument.RootElement.GetProperty("dt").GetInt64().UnixTimeStampToDate();

            if (jsonDocument.RootElement.TryGetProperty("sys", out var sys))
            {
                weatherDetails.Country = sys.GetProperty("country").GetString();
                weatherDetails.Sunrise = sys.GetProperty("sunrise").GetInt64().UnixTimeStampToDate();
                weatherDetails.Sunset = sys.GetProperty("sunset").GetInt64().UnixTimeStampToDate();
            }

            weatherDetails.CityId = jsonDocument.RootElement.GetProperty("id").GetInt32();
            weatherDetails.CityName = jsonDocument.RootElement.GetProperty("name").GetString();

            return weatherDetails;
        }

        private class City
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Country { get; set; }
            public Coord Coord { get; set; }
        }

        private class Coord
        {
            public double Lon { get; set; }
            public double Lat { get; set; }
        }

        private List<City> _cachedCities;
        private int _index;
        
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
                        using(var stream = await response.Content.ReadAsStreamAsync())
                        using( var output = new MemoryStream())
                        using (var sr = new GZipStream(stream, CompressionMode.Decompress))
                        {
                            await sr.CopyToAsync(output, cancellationToken);
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
                    var cities = jsonString.Deserialize<List<City>>();
                    _cachedCities = cities ?? throw new FileHandlerException("The json data parsing returned nothing.");
                    _index = 0;
                }
                catch (Exception ex)
                {
                    throw new FileHandlerException($"The json data could not be parsed.  {ex.Message}", ex);
                }
            }

            if (_index < _cachedCities.Count)
            {    
                var cityItem = _cachedCities[_index];
                var city = new CityDetails()
                {
                    CityId = cityItem.Id,
                    CityName = cityItem.Name,
                    Country = cityItem.Country,
                    Longitude = cityItem.Coord.Lon,
                    Latitude = cityItem.Coord.Lat
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