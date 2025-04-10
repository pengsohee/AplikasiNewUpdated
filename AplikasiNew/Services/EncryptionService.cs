using System.Security.Cryptography;
using System.Text;

namespace AplikasiNew.Services
{
    public interface IEncryptionService
    {
        string Encrypt(string plainText);
        string Decrypt(string cipherText);
        bool IsEncrypted(string data);
    }
    public class EncryptionService : IEncryptionService
    {
        private readonly byte[] _key;
        private readonly byte[] _iv;

        public EncryptionService(IConfiguration config)
        {
            var keyString = config["EncryptionKey"];
            var ivString = config["EncryptionIV"];

            if (string.IsNullOrWhiteSpace(keyString))
                throw new ArgumentException("EncryptionKey is missing or empty in configuration.");

            if (string.IsNullOrWhiteSpace(ivString))
                throw new ArgumentException("EncryptionIV is missing or empty in configuration.");

            _key = Encoding.UTF8.GetBytes(keyString);
            _iv = Encoding.UTF8.GetBytes(ivString);

            if (_key.Length != 32)
                throw new ArgumentException("EncryptionKey must be 32 bytes (256 bits) for AES-256.");

            if (_iv.Length != 16)
                throw new ArgumentException("EncryptionIV must be 16 bytes (128 bits) for AES.");
        }
        public bool IsEncrypted(string data)
        {
            return data.Contains(":");
        }

        public string Encrypt(string plainText)
        {
            if (string.IsNullOrEmpty(plainText))
                return plainText;

            int lengthToEncrypt = (int)Math.Ceiling(plainText.Length * 0.6);
            string partToEncrypt = plainText.Substring(0, lengthToEncrypt);
            string remainingPart = plainText.Substring(lengthToEncrypt);

            using (Aes aes = Aes.Create())
            {
                aes.Key = _key;
                aes.IV = _iv;
                aes.Mode = CipherMode.CBC;
                aes.Padding = PaddingMode.PKCS7;

                using (var encryptor = aes.CreateEncryptor(aes.Key, aes.IV))
                {
                    byte[] plainBytes = Encoding.UTF8.GetBytes(partToEncrypt);
                    byte[] encryptedBytes = encryptor.TransformFinalBlock(plainBytes, 0, plainBytes.Length);
                    string encryptedPart = Convert.ToBase64String(encryptedBytes);

                    return $"{encryptedPart}:{remainingPart}";
                }
            }
        }

        public string Decrypt(string encryptedText)
        {
            if (string.IsNullOrEmpty(encryptedText))
                return encryptedText;

            string[] parts = encryptedText.Split(':');
            if (parts.Length != 2)
                return encryptedText;

            string encryptedPart = parts[0];
            string plainPart = parts[1];

            try
            {
                using (Aes aes = Aes.Create())
                {
                    aes.Key = _key;
                    aes.IV = _iv;
                    aes.Mode = CipherMode.CBC;
                    aes.Padding = PaddingMode.PKCS7;

                    using (var decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
                    {
                        byte[] encryptedBytes = Convert.FromBase64String(encryptedPart);
                        byte[] decryptedBytes = decryptor.TransformFinalBlock(encryptedBytes, 0, encryptedBytes.Length);
                        string decryptedPart = Encoding.UTF8.GetString(decryptedBytes);

                        return decryptedPart + plainPart;
                    }
                }
            }
            catch
            {
                return encryptedText;
            }
        }
    }
}
