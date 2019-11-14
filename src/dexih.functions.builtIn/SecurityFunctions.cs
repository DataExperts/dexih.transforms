using Dexih.Utils.Crypto;

namespace dexih.functions.builtIn
{
    public class SecurityFunctions
    {
        [GlobalSettings]
        public GlobalSettings GlobalSettings { get; set; }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Manual Encrypt",
            Description = "Encrypts the string using the key string.  More iterations = stronger/slower")]
        public string Encrypt(string value, string key, int iterations)
        {
            return EncryptString.Encrypt(value, key, iterations);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Manual Decrypt",
            Description = "Decrypts the string using the key string and iteractions.  More iterations = stronger/slower encrypt.")]
        public string Decrypt(string value, string key, int iterations)
        {
            return EncryptString.Decrypt(value, key, iterations);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Strong Encrypt",
            Description = "Strong Encrypts the string.")]
        public string StrongEncrypt(string value)
        {
            return EncryptString.Encrypt(value, GlobalSettings?.EncryptionKey, 1000);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Strong Decrypt",
            Description = "Strong Decrypts the string.")]
        public string StrongDecrypt(string value)
        {
            return EncryptString.Decrypt(value, GlobalSettings?.EncryptionKey, 1000);
        }
        
        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Fast Encrypt",
            Description = "Fast Encrypts the string.")]
        public string FastEncrypt(string value)
        {
            return EncryptString.Encrypt(value, GlobalSettings?.EncryptionKey, 5);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Fast Decrypt",
            Description = "Fast Decrypts the string.")]
        public string FastDecrypt(string value)
        {
            return EncryptString.Decrypt(value, GlobalSettings?.EncryptionKey, 5);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Secure Hash",
            Description =
                "Creates a random-salted, SHA256 hash of the string.  This is secure and can be used for passwords and other sensative data.  This can only be validated using the Validate Hash function.")]
        public string SecureHash(string value)
        {
            return HashString.CreateHash(value);
        }

        [TransformFunction(FunctionType = EFunctionType.Map, Category = "Security", Name = "Validate Secure Hash",
            Description = "Validates a value created from the Secure Hash function.")]
        public bool ValidateSecureHash(string value, string hash)
        {
            return HashString.ValidateHash(value, hash);
        }
    }
}