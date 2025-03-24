using System.Security.Cryptography;
using System.Text;
using AplikasiNew.Models;
using Dapper;
using Npgsql;

namespace AplikasiNew.Services;

public class DataService(IConfiguration config)
{
#pragma warning disable CS8601 // Possible null reference assignment.
    private readonly string _sourceDb = config.GetConnectionString("SourceDB");
#pragma warning restore CS8601 // Possible null reference assignment.
#pragma warning disable CS8601 // Possible null reference assignment.
    private readonly string _targetDb = config.GetConnectionString("TargetDB");
#pragma warning restore CS8601 // Possible null reference assignment.
#pragma warning disable CS8604 // Possible null reference argument.
    private readonly byte[] _key = Encoding.UTF8.GetBytes(config["EncryptionKey"]);
#pragma warning restore CS8604 // Possible null reference argument.
#pragma warning disable CS8604 // Possible null reference argument.
    private readonly byte[] _iv = Encoding.UTF8.GetBytes(config["EncryptionIV"]);
#pragma warning restore CS8604 // Possible null reference argument.
    private readonly List<string> _encryptedFields = config.GetSection("EncryptionFields").Get<List<string>>() ?? new List<string>();

    private string Encrypt(string plainText)
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

    private string Decrypt(string encryptedText)
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

    private bool IsEncrypted(string data)
    {
        return data.Contains(":");
    }

    public async Task TransferData(string sourceTable)
    {
        using var sourceConn = new NpgsqlConnection(_sourceDb);
        using var targetConn = new NpgsqlConnection(_targetDb);

        var users = await sourceConn.QueryAsync<UserData>($"SELECT id, username, email, password, credit_card_token FROM {sourceTable}");

        foreach (var user in users)
        {
            foreach (var field in _encryptedFields)
            {
                var property = typeof(UserData).GetProperty(field, System.Reflection.BindingFlags.IgnoreCase | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                if (property != null)
                {
                    var value = property.GetValue(user)?.ToString();
                    if (!string.IsNullOrEmpty(value) && !IsEncrypted(value))
                    {
                        property.SetValue(user, Encrypt(value));
                    }
                }
            }

            var existing = await targetConn.QueryFirstOrDefaultAsync<int?>(
                "SELECT id FROM new_app WHERE id = @Id", new { user.Id });

            if (existing == null)
            {
                await targetConn.ExecuteAsync(
                    "INSERT INTO new_app (id, username, email, password, credit_card_token) VALUES (@Id, @Username, @Email, @Password, @CreditCardToken)",
                    user
                );
            }
            else
            {
                Console.WriteLine($"User with ID {user.Id} already exists. Skipping insertion.");
            }
        }
    }

    public async Task<IEnumerable<UserData>> GetData()
    {
        using var targetConn = new NpgsqlConnection(_targetDb);
        return await targetConn.QueryAsync<UserData>("SELECT id, username, email, password, credit_card_token FROM new_app");
    }

    public async Task<IEnumerable<UserData>> DetokenizeData()
    {
        var data = await GetData();
        return data.Select(user =>
        {
            var newUser = new UserData
            {
                Id = user.Id,
                Username = user.Username
            };

            foreach (var field in _encryptedFields)
            {
                var property = typeof(UserData).GetProperty(field, System.Reflection.BindingFlags.IgnoreCase | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                if (property != null)
                {
                    var value = property.GetValue(user)?.ToString();
                    if (!string.IsNullOrEmpty(value) && IsEncrypted(value))
                    {
                        property.SetValue(newUser, Decrypt(value));
                    }
                    else
                    {
                        property.SetValue(newUser, value);
                    }
                }
            }

            return newUser;
        });
    }
}
