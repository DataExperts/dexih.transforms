using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Exceptions;
using dexih.transforms;
using Newtonsoft.Json.Linq;

namespace dexih.functions.external
{
    public class OpenWeatherMap
    {
        public enum TemperatureScale
        {
            Fahrenheit, Celsius, Kelvin
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

        private string GetOpenWeatherUrl(string key)
        {
            return $"http://api.openweathermap.org/data/2.5/weather?APPID={key}";
        }
        
        private async Task<(string url, string statusCode, bool isSuccess, Stream response)> GetWebServiceResponse(string key, string parameters, CancellationToken cancellationToken)
        {
            var uri = new Uri(GetOpenWeatherUrl(key) + parameters);
            
            // var uri = new Uri("https://samples.openweathermap.org/data/2.5/weather?q=London,uk&appid=b6907d289e10d714a6e88b30761fae22");

            using (var client = new HttpClient())
            {
				client.BaseAddress = new Uri(uri.GetLeftPart(UriPartial.Authority));
                client.DefaultRequestHeaders.Accept.Clear();

				var response = await client.GetAsync(uri.PathAndQuery, cancellationToken);

				return (uri.ToString(), response.StatusCode.ToString(), response.IsSuccessStatusCode, await response.Content.ReadAsStreamAsync());
            }
        }
        
        private double? ConvertTemperature(double? kelvin, TemperatureScale temperatureScale)
        {
            if (kelvin == null) return null;
            
            switch (temperatureScale)
            {
                case TemperatureScale.Fahrenheit:
                    return ((9.0 / 5.0) * (kelvin - 273.15)) + 32;
                case TemperatureScale.Celsius:
                    return kelvin - 273.15;
                case TemperatureScale.Kelvin:
                    return kelvin;
                default:
                    throw new ArgumentOutOfRangeException(nameof(temperatureScale), temperatureScale, null);
            }
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Weather", Name = "Weather By City Name",
            Description = "Gets the weather based on a city name.  Sign up for key at [openweathermap.org](https://openweathermap.org/price).")]
        public Task<WeatherDetails> WeatherByCityName(string key, string cityName,
            TemperatureScale temperatureScale)
        {
            return GetWeatherResponse(key, $"&q={cityName}", temperatureScale);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Weather", Name = "Weather By Coordinates",
            Description = "Gets the weather based on a city id.  Sign up for key at [openweathermap.org](https://openweathermap.org/price).")]
        public Task<WeatherDetails> WeatherByCityId(string key, int cityId,
            TemperatureScale temperatureScale)
        {
            return GetWeatherResponse(key, $"&id={cityId}", temperatureScale);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Weather", Name = "Weather By City Id",
            Description = "Gets the weather based on a longitude and latitude.  Sign up for key at [openweathermap.org](https://openweathermap.org/price).")]
        public Task<WeatherDetails> WeatherByCityId(string key, double latitude, double longitude,
            TemperatureScale temperatureScale)
        {
            return GetWeatherResponse(key, $"&lat={latitude}&lon={longitude}", temperatureScale);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Weather", Name = "Weather By City Id",
            Description = "Gets the weather based on a zip code.  Note if country is not specified when USA will be default.  Sign up for key at [openweathermap.org](https://openweathermap.org/price).")]
        public Task<WeatherDetails> WeatherByCityId(string key, string zipCode, string country,
            TemperatureScale temperatureScale)
        {
            return GetWeatherResponse(key, $"&zip={zipCode},{country}", temperatureScale);
        }


        private async Task<WeatherDetails> GetWeatherResponse(string key, string uri, TemperatureScale temperatureScale)
        {
            var (url, statusCode, isSuccess, response) = await GetWebServiceResponse(key, uri, CancellationToken.None);

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

        private IEnumerator<JToken> _cachedCities;
        
        [TransformFunction(FunctionType = EFunctionType.Rows, Category = "Weather", Name = "Weather City List",
            Description = "Get a list of all the cities used for the weather functions.  Data from [openweathermap.org](https://openweathermap.org)")]
        public async Task<CityDetails> WeatherCities()
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

                    var response = await client.GetAsync(uri.PathAndQuery, CancellationToken.None);

                    if (response.IsSuccessStatusCode)
                    {
                        var stream = await response.Content.ReadAsStreamAsync();
                        using (MemoryStream output = new MemoryStream())
                        using (GZipStream sr = new GZipStream(stream, CompressionMode.Decompress))
                        {
                            sr.CopyTo(output);

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
                        _cachedCities = jArray.Children().GetEnumerator();
                    }
                }
                catch (Exception ex)
                {
                    throw new FileHandlerException($"The json data could not be parsed.  {ex.Message}", ex);
                }

            }

            if (_cachedCities == null) return null;
            if(!_cachedCities.MoveNext()) return null;
            var token = _cachedCities.Current;

            var city = new CityDetails()
            {
                CityId = token["id"].Value<int>(),
                CityName = token["name"].Value<string>(),
                Country = token["country"].Value<string>(),
                Longitude = token["coord"]?["lon"].Value<double>() ?? 0,
                Latitude = token["coord"]?["lat"].Value<double>() ?? 0,
            };


            return city;
        }
    }
}