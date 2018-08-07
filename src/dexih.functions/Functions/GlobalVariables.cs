using System.Collections.Generic;

namespace dexih.functions
{
    public class GlobalVariables
    {
        // used by encrypt functions.
        public string EncryptionKey { get; set; }

        private readonly Dictionary<string, object> _variables;

        public GlobalVariables(string encryptionKey)
        {
            EncryptionKey = encryptionKey;
            _variables = new Dictionary<string, object>();
        }

        public void AddVariable(string name, object value)
        {
            _variables.Add(name, value);
        }
        
        public object this[string name] => _variables[name];

    }
}