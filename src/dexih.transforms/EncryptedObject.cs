using System;
using Newtonsoft.Json;

namespace dexih.transforms
{
    /// <summary>
    /// This is used to transport an encrypted value and the plaintext value through the transforms
    /// which allows the plaintext to be used for the delta comparison.
    /// </summary>
    public class EncryptedObject: IEquatable<string>
    {
        [JsonIgnore]
        public object OriginalValue {get;set;}
        
        public string EncryptedValue {get;set;}
		
        public EncryptedObject(object originalValue, string encryptedValue)
        {
            OriginalValue = originalValue;
            EncryptedValue = encryptedValue;
        }
		
        public bool Equals(string value) 
        {
            return value == EncryptedValue;
        }
		
        public override string ToString()
        {
            return EncryptedValue;
        }
    }
    
}