using System.Security.Cryptography;
using System.Text;
using AplikasiNew.Models;
using Dapper;
using Microsoft.AspNetCore.Components.Server;
using Microsoft.AspNetCore.Http.HttpResults;
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

    public async Task<bool> CheckTable(NpgsqlConnection conn, string sourceTable, string schema = "public")
    {
        string query = @"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @schema AND table_name = @sourceTable";
        Console.WriteLine($"schema: {schema}, sourceTable: {sourceTable}");
        long count = await conn.ExecuteScalarAsync<long>(query, new { schema, sourceTable });
        return count > 0;
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

    // Transfer data to other table
    public async Task TransferTable(string SourceConnection, string TargetConnection, string SourceTable, string TargetTable, List<string> Columns)
    {
        using var sourceConn = new NpgsqlConnection(SourceConnection);
        using var targetConn = new NpgsqlConnection(TargetConnection);

        var selectQuery = $"SELECT * FROM {SourceTable}";
        var sourceData = await sourceConn.QueryAsync<dynamic>(selectQuery);

        var processedRows = new List<dynamic>();
        foreach (var row in sourceData)
        {
            var rowDict = row as IDictionary<string, object>;
            if (rowDict == null)
                continue;

            foreach (var field in Columns)
            {
                if (rowDict.ContainsKey(field))
                {
                    var value = rowDict[field]?.ToString();
                    if (!string.IsNullOrEmpty(value) && !IsEncrypted(value))
                    {
                        rowDict[field] = Encrypt(value);
                    }
                }
            }
            processedRows.Add(rowDict);
        }

        foreach (var row in processedRows)
        {
            var dict = row as IDictionary<string, object>;
            if (dict == null || dict.Count == 0)
                continue;

            var columns = string.Join(", ", dict.Keys);
            var parameters = string.Join(", ", dict.Keys.Select(c => "@" + c));
            var insertQuery = $"INSERT INTO {TargetTable} ({columns}) VALUES ({parameters})";

            await targetConn.ExecuteAsync(insertQuery, dict);
        }
    }

    // Backup table
    public async Task BackupTable(string sourceTable, string schema = "public")
    {
        using var sourceConn = new NpgsqlConnection(_sourceDb);
        string backupTable = sourceTable + "_backup";

        // Check if backup database exist
        bool exists = await CheckTable(sourceConn, backupTable);
        Console.WriteLine($"The table {sourceTable}: {exists}"); // debug

        if (!exists)
        {
            Console.WriteLine($"The table {backupTable} doesn't exist. Creating...");
            // create table
            string query = $@"CREATE TABLE IF NOT EXISTS ""{schema}"".""{backupTable}"" AS TABLE ""{schema}"".""{sourceTable}"" WITH NO DATA;";
            await sourceConn.ExecuteAsync(query);
        }

        // Inserting data to table
        Console.WriteLine($"The table {backupTable} exists. Updating...");
        string copyQuery = $@"INSERT INTO ""{schema}"".""{backupTable}"" SELECT * FROM ""{schema}"".""{sourceTable}"";";
        await sourceConn.ExecuteAsync(copyQuery);

        // Creating primary key for backup table
        Console.WriteLine($"The data has been inserted to the table: {backupTable}. Creating Primary Key...");
        string PKQuery = $@"ALTER TABLE ""{schema}"".""{backupTable}"" ADD PRIMARY KEY (id);";
        await sourceConn.ExecuteAsync(PKQuery);

        Console.WriteLine($"The backup table for {sourceTable} has been successfully created");
    }

    // Tokenize table with selected columns
    public async Task TokenizeTable(string sourceTable, List<string> columns)
    {
        // Ensure the "id" column is always present for update reference.
        if (!columns.Any(c => string.Equals(c, "id", StringComparison.OrdinalIgnoreCase)))
        {
            columns.Add("id");
        }

        using var sourceConn = new NpgsqlConnection(_sourceDb);

        var selectedColumns = string.Join(", ", columns);
        Console.WriteLine($"The selected columns are {selectedColumns}");

        var selectQuery = $"SELECT {selectedColumns} FROM {sourceTable}";
        var users = await sourceConn.QueryAsync<dynamic>(selectQuery);

        foreach (var user in users)
        {
            // dictionary
            var userDict = user as IDictionary<string, object>;

            foreach (var field in _encryptedFields)
            {
                // check if columns are available
                if (columns.Any(c => string.Equals(c, field, StringComparison.OrdinalIgnoreCase)) && userDict.ContainsKey(field))
                {
                    var curr = userDict[field]?.ToString();
                    if (!string.IsNullOrEmpty(curr) && !IsEncrypted(curr))
                    {
                        userDict[field] = Encrypt(curr);
                    }
                }
            }

            // dynamic set clause for update query
            var setClause = string.Join(", ", columns.Where(c => !string.Equals(c, "id", StringComparison.OrdinalIgnoreCase)).Select(c => $"{c} = @{c}"));
            var updateQuery = $"UPDATE {sourceTable} SET {setClause} WHERE id = @id";

            try
            {
                await sourceConn.ExecuteAsync(updateQuery, userDict);
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                Console.WriteLine($"Duplicate key error while updating user. Details: {ex.Message}");
            }
            
        }
    }

    // Detokenize whole table
    public async Task DetokenizeTable(string sourceTable, List<string> columns)
    {
        if (!columns.Any(c => string.Equals(c, "id", StringComparison.OrdinalIgnoreCase)))
        {
            columns.Add("id");
        }

        using var sourceConn = new NpgsqlConnection(_sourceDb);

        var selectedColumns = string.Join(", ", columns);
        Console.WriteLine($"The selected columns are {selectedColumns}");

        var selectQuery = $"SELECT {selectedColumns} FROM {sourceTable}";
        var users = await sourceConn.QueryAsync<dynamic>(selectQuery);

        foreach (var user in users)
        {
            var userDict = user as IDictionary<string, object>;

            foreach (var field in _encryptedFields)
            {
                // check if columns are available
                if (columns.Any(c => string.Equals(c, field, StringComparison.OrdinalIgnoreCase)) && userDict.ContainsKey(field))
                {
                    var curr = userDict[field]?.ToString();
                    if (!string.IsNullOrEmpty(curr) && IsEncrypted(curr))
                    {
                        userDict[field] = Decrypt(curr);
                    }
                }
            }
            // dynamic set clause for update query
            var setClause = string.Join(", ", columns.Where(c => !string.Equals(c, "id", StringComparison.OrdinalIgnoreCase)).Select(c => $"{c} = @{c}"));
            var updateQuery = $"UPDATE {sourceTable} SET {setClause} WHERE id = @id";

            try
            {
                await sourceConn.ExecuteAsync(updateQuery, userDict);
            }
            catch (PostgresException ex) when (ex.SqlState == "23505")
            {
                Console.WriteLine($"Duplicate key error while updating user. Details: {ex.Message}");
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
