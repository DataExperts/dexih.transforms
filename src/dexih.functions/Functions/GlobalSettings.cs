using System.Collections.Generic;
using System.Net.Http;

namespace dexih.functions
{
    /// <summary>
    /// Settings and variables which apply to anything running on the remote agent.
    /// </summary>
    public class GlobalSettings
    {
        // used by encrypt functions.
        public string EncryptionKey { get; set; }

        public FilePermissions FilePermissions { get; set; }

        private readonly Dictionary<string, object> _variables;

        public GlobalSettings(string encryptionKey)
        {
            EncryptionKey = encryptionKey;
            _variables = new Dictionary<string, object>();
        }
        
        public IHttpClientFactory HttpClientFactory { get; set; }

        public GlobalSettings()
        {
            _variables = new Dictionary<string, object>();
        }

        public void AddVariable(string name, object value)
        {
            _variables.Add(name, value);
        }
        
        public object this[string name] => _variables[name];

    }
}