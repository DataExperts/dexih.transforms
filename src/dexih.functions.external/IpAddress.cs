using System;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions.Exceptions;

namespace dexih.functions.external
{
    public class IpAddress : IDisposable
    {
        private const string KeyDescription =
            "Sign up for key at [ipstack.com](https://ipstack.com/).";

        [GlobalSettings]
        public GlobalSettings GlobalSettings { get; set; }
        
        public class IpStack
        {
            public string ip { get; set; }
            public string type { get; set; }
            public string continent_code { get; set; }
            public string continent_name { get; set; }
            public string country_code { get; set; }
            public string country_name { get; set; }
            public string region_code { get; set; }
            public string city { get; set; }
            public string zip { get; set; }
            public double latitude { get; set; }
            public double longitude { get; set; }
            public Location location { get; set; }

            public class Location
            {
                public string geoname_id { get; set; }
                public string capital { get; set; }
                public Language[] languages { get; set; }
                public string country_flag { get; set; }
                public string country_flag_emoji { get; set; }
                public string country_flag_emoji_unicode { get; set; }
                public string calling_code { get; set; }
                public bool is_eu { get; set; }
            }

            public class Language
            {
                public string code { get; set; }
                public string name { get; set; }
                public string native { get; set; }
            }
        }

        public class IpStackError
        {
            public bool success { get; set; }
            public IpStackErrorMessage error { get; set; }
        }

        public class IpStackErrorMessage
        {
            public int code { get; set; }
            public string type { get; set; }
            public string info { get; set; }
        }

        public class IpDetails
        {
            public string type { get; set; }
            public string continent_code { get; set; }
            public string continent_name { get; set; }
            public string country_code { get; set; }
            public string country_name { get; set; }
            public string region_code { get; set; }
            public string city { get; set; }
            public string zip { get; set; }
            public double latitude { get; set; }
            public double longitude { get; set; }
            public string language_code { get; set; }
            public string language_name { get; set; }
            public bool is_eu { get; set; }
        }

        private string GetIPStackUrl(string ip, string key)
        {
            return
                $"http://api.ipstack.com/{ip}?access_key={key}";
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "IPAddress", Name = "Ip to Geolocation",
            Description = "Converts an IP Address to a geolocation.")]
        public async Task<IpDetails> IpToGeolocation(
            [TransformFunctionParameter(Description = KeyDescription), TransformFunctionPassword]
            string key,
            string ip,
            CancellationToken cancellationToken = default)
        {
            var url = GetIPStackUrl(ip, key);

            var httpClient = GlobalSettings.HttpClientFactory.CreateClient();
                
            var response = await httpClient.GetAsync(url, cancellationToken);

            if (response.IsSuccessStatusCode)
            {
                var ipAddress = await JsonSerializer.DeserializeAsync<IpStack>(await response.Content.ReadAsStreamAsync(), cancellationToken: cancellationToken);

                return new IpDetails()
                {
                    city = ipAddress.city,
                    continent_code = ipAddress.continent_code,
                    continent_name = ipAddress.continent_name,
                    country_code = ipAddress.country_code,
                    country_name = ipAddress.country_name,
                    is_eu = ipAddress.location.is_eu,
                    language_code = ipAddress.location.languages[0].code,
                    language_name = ipAddress.location.languages[0].name,
                    latitude = ipAddress.latitude,
                    longitude = ipAddress.longitude,
                    region_code = ipAddress.region_code,
                    type = ipAddress.type,
                    zip = ipAddress.zip
                };
            }
            else
            {
                var element = await JsonSerializer.DeserializeAsync<IpStackError>(await response.Content.ReadAsStreamAsync(), cancellationToken: cancellationToken);
                throw new FunctionException($"The IpToGeolocation failed.  Message: {element.error.info}");
            }
        }
        
        public void Dispose()
        {
            //_httpClient?.Dispose();
        }
    }
}