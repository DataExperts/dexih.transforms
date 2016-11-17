using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace dexih.functions
{
    /// <summary>
    /// Code from http://stackoverflow.com/questions/10168240/encrypting-decrypting-a-string-in-c-sharp
    /// </summary>
    public class EncryptString
    {
        // This constant is used to determine the keysize of the encryption algorithm in bits.
        // We divide this by 8 within the code below to get the equivalent number of bytes.
        private const int Keysize = 128;

        // This constant determines the number of iterations for the password bytes generation function.
        //private const int DerivationIterations = 1000;

        public static string GenerateRandomKey(int length = 50)
        {
            byte[] randomBytes = new byte[length];

            using (var randomNumber = RandomNumberGenerator.Create())
            {
                randomNumber.GetBytes(randomBytes);
            }

            return Convert.ToBase64String(randomBytes);
        }

        /// <summary>
        /// Encrypts the string value using the passPhase as the encryption key.
        /// </summary>
        /// <param name="plainText">String to encrypt</param>
        /// <param name="passPhrase">Encryption Key</param>
        /// <returns></returns>
        public static ReturnValue<string> Encrypt(string plainText, string passPhrase, int DerivationIterations)
        {
            try
            {
                if (string.IsNullOrEmpty(plainText)) return new ReturnValue<string>(true, "");

                // Salt and IV is randomly generated each time, but is prepended to encrypted cipher text
                // so that the same Salt and IV values can be used when decrypting.  
                var saltStringBytes = Generate256BitsOfRandomEntropy();
                var ivStringBytes = Generate256BitsOfRandomEntropy();
                var plainTextBytes = Encoding.UTF8.GetBytes(plainText);
                using (var password = new Rfc2898DeriveBytes(passPhrase, saltStringBytes, DerivationIterations))
                {
                    var keyBytes = password.GetBytes(Keysize / 8);
                    using (var symmetricKey = Aes.Create()) // not supported in .net core =new RijndaelManaged())
                    {
                        symmetricKey.BlockSize = Keysize;
                        symmetricKey.Mode = CipherMode.CBC;
                        symmetricKey.Padding = PaddingMode.PKCS7;
                        using (var encryptor = symmetricKey.CreateEncryptor(keyBytes, ivStringBytes))
                        {
                            using (var memoryStream = new MemoryStream())
                            {
                                using (var cryptoStream = new CryptoStream(memoryStream, encryptor, CryptoStreamMode.Write))
                                {
                                    cryptoStream.Write(plainTextBytes, 0, plainTextBytes.Length);
                                    cryptoStream.FlushFinalBlock();
                                    // Create the final bytes as a concatenation of the random salt bytes, the random iv bytes and the cipher bytes.
                                    var cipherTextBytes = saltStringBytes;
                                    cipherTextBytes = cipherTextBytes.Concat(ivStringBytes).ToArray();
                                    cipherTextBytes = cipherTextBytes.Concat(memoryStream.ToArray()).ToArray();
                                    memoryStream.Dispose();
                                    cryptoStream.Dispose();
                                    return new ReturnValue<string>(true, Convert.ToBase64String(cipherTextBytes));
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                return new ReturnValue<string>(false, "Encryption failed due to: " + ex.Message, ex);
            }
        }


        /// <summary>
        /// Decrypts a string using the encryption key.
        /// </summary>
        /// <param name="cipherText">The encrypted value to decrypt.</param>
        /// <param name="passPhrase">The encryption key used to initially encrypt the string.</param>
        /// <returns></returns>
        public static ReturnValue<string> Decrypt(string cipherText, string passPhrase, int DerivationIterations)
        {
            try
            {
                if (string.IsNullOrEmpty(cipherText)) return new ReturnValue<string>(true, "");


                // Get the complete stream of bytes that represent:
                // [32 bytes of Salt] + [32 bytes of IV] + [n bytes of CipherText]
                var cipherTextBytesWithSaltAndIv = Convert.FromBase64String(cipherText);
                // Get the saltbytes by extracting the first 32 bytes from the supplied cipherText bytes.
                var saltStringBytes = cipherTextBytesWithSaltAndIv.Take(Keysize / 8).ToArray();
                // Get the IV bytes by extracting the next 32 bytes from the supplied cipherText bytes.
                var ivStringBytes = cipherTextBytesWithSaltAndIv.Skip(Keysize / 8).Take(Keysize / 8).ToArray();
                // Get the actual cipher text bytes by removing the first 64 bytes from the cipherText string.
                var cipherTextBytes = cipherTextBytesWithSaltAndIv.Skip((Keysize / 8) * 2).Take(cipherTextBytesWithSaltAndIv.Length - ((Keysize / 8) * 2)).ToArray();

                using (var password = new Rfc2898DeriveBytes(passPhrase, saltStringBytes, DerivationIterations))
                {
                    var keyBytes = password.GetBytes(Keysize / 8);
                    using (var symmetricKey = Aes.Create()) // not supported in .net core =  new RijndaelManaged())
                    {
                        symmetricKey.BlockSize = Keysize;
                        symmetricKey.Mode = CipherMode.CBC;
                        symmetricKey.Padding = PaddingMode.PKCS7;
                        using (var decryptor = symmetricKey.CreateDecryptor(keyBytes, ivStringBytes))
                        {
                            using (var memoryStream = new MemoryStream(cipherTextBytes))
                            {
                                using (var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read))
                                {
                                    var plainTextBytes = new byte[cipherTextBytes.Length];
                                    var decryptedByteCount = cryptoStream.Read(plainTextBytes, 0, plainTextBytes.Length);
                                    memoryStream.Dispose();
                                    cryptoStream.Dispose();
                                    return new ReturnValue<string>(true, Encoding.UTF8.GetString(plainTextBytes, 0, decryptedByteCount));
                                }
                            }
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                return new ReturnValue<string>(false, "Decryption failed due to: " + ex.Message, ex);
            }
        }

        private static byte[] Generate256BitsOfRandomEntropy()
        {
            var randomBytes = new byte[Keysize/8]; // 32 Bytes will give us 256 bits.
            using (var rngCsp = RandomNumberGenerator.Create()) // // not supported in .net core new RNGCryptoServiceProvider())
            {
                // Fill the array with cryptographically secure random bytes.
                rngCsp.GetBytes(randomBytes);
            }
            return randomBytes;
        }
    }
}
