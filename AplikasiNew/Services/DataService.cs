using System.Linq.Expressions;
using System.Security.Cryptography;
using System.Text;
using AplikasiNew.Exceptions;
using AplikasiNew.Models;
using Dapper;
using Microsoft.AspNetCore.Components.Server;
using Microsoft.AspNetCore.Http.HttpResults;
using Npgsql;

namespace AplikasiNew.Services;

public class DataService(IConfiguration config)
{
#pragma warning disable CS8604 // Possible null reference argument.
    private readonly byte[] _key = Encoding.UTF8.GetBytes(config["EncryptionKey"]);
#pragma warning restore CS8604 // Possible null reference argument.
#pragma warning disable CS8604 // Possible null reference argument.
    private readonly byte[] _iv = Encoding.UTF8.GetBytes(config["EncryptionIV"]);
#pragma warning restore CS8604 // Possible null reference argument.
    private readonly List<string> _encryptedFields = config.GetSection("EncryptionFields").Get<List<string>>() ?? new List<string>();
    //private bool IsEncrypted(string data)
    //{
    //    return data.Contains(":");
    //}
    //private string Encrypt(string plainText)
    //{
    //    if (string.IsNullOrEmpty(plainText))
    //        return plainText;

    //    int lengthToEncrypt = (int)Math.Ceiling(plainText.Length * 0.6);
    //    string partToEncrypt = plainText.Substring(0, lengthToEncrypt);
    //    string remainingPart = plainText.Substring(lengthToEncrypt);

    //    using (Aes aes = Aes.Create())
    //    {
    //        aes.Key = _key;
    //        aes.IV = _iv;
    //        aes.Mode = CipherMode.CBC;
    //        aes.Padding = PaddingMode.PKCS7;

    //        using (var encryptor = aes.CreateEncryptor(aes.Key, aes.IV))
    //        {
    //            byte[] plainBytes = Encoding.UTF8.GetBytes(partToEncrypt);
    //            byte[] encryptedBytes = encryptor.TransformFinalBlock(plainBytes, 0, plainBytes.Length);
    //            string encryptedPart = Convert.ToBase64String(encryptedBytes);

    //            return $"{encryptedPart}:{remainingPart}";
    //        }
    //    }
    //}

    //private string Decrypt(string encryptedText)
    //{
    //    if (string.IsNullOrEmpty(encryptedText))
    //        return encryptedText;

    //    string[] parts = encryptedText.Split(':');
    //    if (parts.Length != 2)
    //        return encryptedText;

    //    string encryptedPart = parts[0];
    //    string plainPart = parts[1];

    //    try
    //    {
    //        using (Aes aes = Aes.Create())
    //        {
    //            aes.Key = _key;
    //            aes.IV = _iv;
    //            aes.Mode = CipherMode.CBC;
    //            aes.Padding = PaddingMode.PKCS7;

    //            using (var decryptor = aes.CreateDecryptor(aes.Key, aes.IV))
    //            {
    //                byte[] encryptedBytes = Convert.FromBase64String(encryptedPart);
    //                byte[] decryptedBytes = decryptor.TransformFinalBlock(encryptedBytes, 0, encryptedBytes.Length);
    //                string decryptedPart = Encoding.UTF8.GetString(decryptedBytes);

    //                return decryptedPart + plainPart;
    //            }
    //        }
    //    }
    //    catch
    //    {
    //        return encryptedText;
    //    }
    //}

    public void ValidateConnectionString(string connectionString)
    {
        try
        {
            var builder = new Npgsql.NpgsqlConnectionStringBuilder(connectionString);
            using var connection = new Npgsql.NpgsqlConnection(connectionString);
            connection.Open(); 
        }
        // Invalid Authentication
        catch (Npgsql.PostgresException ex) when (ex.SqlState == "28P01") 
        {
            throw new InvalidConnectionStringException("The provided connection can't reach the database because of authentication failure.");
        }
        // Network isues
        catch (Npgsql.NpgsqlException ex) when (ex.InnerException is System.Net.Sockets.SocketException) 
        {
            throw new DatabaseNetworkException("Network-related error occurred while establishing a connection.", ex);
        }
        // Invalid Connection String
        catch (Exception ex)
        {
            throw new InvalidConnectionStringException("The provided connection string is invalid or cannot connect to the database.", ex);
        }
    }

    public void ValidateTable(string connectionString)
    {
        try
        {
            var builder = new Npgsql.NpgsqlConnectionStringBuilder(connectionString);
            using var connection = new Npgsql.NpgsqlConnection(connectionString);
            connection.Open();
        }
        // Table Issues
        catch (Npgsql.PostgresException ex) when (ex.SqlState == "42P01")
        {
            throw new InvalidTableException("The provided table does not exist.", ex);
        }        
    }

    public async Task<IEnumerable<DatabaseSchema>> GetTableSchemaAsync(NpgsqlConnection connection, string schema, string tableName)
    {
        string query = @"
        SELECT 
            column_name as ColumnName,
            data_type as DataType,
            (is_nullable = 'YES') as IsNullable
        FROM information_schema.columns 
        WHERE table_schema = @Schema AND table_name = @TableName
        ORDER BY ordinal_position";

        return await connection.QueryAsync<DatabaseSchema>(query, new { Schema = schema, TableName = tableName });
    }

    public async Task CheckSchemaMismatchAsync(NpgsqlConnection sourceConn, NpgsqlConnection targetConn, string schema, string sourceTable, string targetTable)
    {
        var sourceColumns = (await GetTableSchemaAsync(sourceConn, schema, sourceTable)).ToList();
        var targetColumns = (await GetTableSchemaAsync(targetConn, schema, targetTable)).ToList();

        // Check column count
        if (sourceColumns.Count != targetColumns.Count)
        {
            throw new SchemaMismatchException($"Mismatch in number of columns: Source table '{sourceTable}' has {sourceColumns.Count}, while target table '{targetTable}' has {targetColumns.Count} columns.");
        }

        // Match column name
        foreach (var srcColumn in sourceColumns)
        {
            var targetColumn = targetColumns.FirstOrDefault(tc => tc.ColumnName.Equals(srcColumn.ColumnName, StringComparison.OrdinalIgnoreCase));
            if (targetColumn == null)
            {
                throw new SchemaMismatchException($"Column '{srcColumn.ColumnName}' exists in source table '{sourceTable}' but not in target table '{targetTable}'.");
            }

            // Compare data type
            if (!srcColumn.DataType.Equals(targetColumn.DataType, StringComparison.OrdinalIgnoreCase))
            {
                throw new SchemaMismatchException($"Data type mismatch for column '{srcColumn.ColumnName}': Source table has '{srcColumn.DataType}' but target table has '{targetColumn.DataType}'.");
            }

            // Compare nullable
            if (srcColumn.IsNullable != targetColumn.IsNullable)
            {
                throw new SchemaMismatchException($"Nullability mismatch for column '{srcColumn.ColumnName}': Source table indicates IsNullable={srcColumn.IsNullable} but target table indicates IsNullable={targetColumn.IsNullable}.");
            }
        }
    }

    public async Task InsertRecordAsync(string connectionString, string query, object parameters)
    {
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        try
        {
            await connection.ExecuteAsync(query, parameters);
        }
        catch (PostgresException ex) when (ex.SqlState == "23505")  // Unique violation in PostgreSQL
        {
            throw new DataIntegrityViolationException("A record with the same unique key already exists.", ex);
        }
        catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
        {
            // General integrity constraint violation codes start with 23
            throw new DataIntegrityViolationException("A data integrity violation occurred.", ex);
        }
    }

    public async Task<bool> CheckTable(NpgsqlConnection conn, string sourceTable, string schema = "public")
    {
        string query = @"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @schema AND table_name = @sourceTable";
        Console.WriteLine($"schema: {schema}, sourceTable: {sourceTable}");
        long count = await conn.ExecuteScalarAsync<long>(query, new { schema, sourceTable });
        return count > 0;
    }

    // MAIN FUNCTION START

    // Transfer data to other table (tokenization)
    public async Task TransferTable(string sourceConnection, string targetConnection, string sourceTable, string targetTable, List<string> columns, int maxAllowedRows = 100000)
    {
        // Validate connection string
        ValidateConnectionString(sourceConnection);
        ValidateConnectionString(targetConnection);
        using var sourceConn = new NpgsqlConnection(sourceConnection);
        using var targetConn = new NpgsqlConnection(targetConnection);

        // Validate Schema Mismatch
        await CheckSchemaMismatchAsync(sourceConn, targetConn, "public", sourceTable, targetTable);

        // Check row count first to simulate large data volume handling
        var countQuery = $"SELECT COUNT(*) FROM \"public\".\"{sourceTable}\"";
        int rowCount = await sourceConn.ExecuteScalarAsync<int>(countQuery);

        if (rowCount > maxAllowedRows)
        {
            throw new LargeDataVolumeException(
                $"The table '{sourceTable}' has {rowCount} rows which exceeds the maximum allowed {maxAllowedRows} for a single transfer operation.");
        }

        var selectQuery = $"SELECT * FROM {sourceTable}";
        var sourceData = await sourceConn.QueryAsync<dynamic>(selectQuery);

        // creates an empty list
        var processedRows = new List<dynamic>();
        try
        {
            foreach (var row in sourceData)
            {
                // extract row to dictionary
                var rowDict = row as IDictionary<string, object>;
                if (rowDict == null)
                    continue; // skips to next iteration

                foreach (var field in columns)
                {
                    if (rowDict.ContainsKey(field))
                    {
                        var value = rowDict[field]?.ToString();
                        if (!string.IsNullOrEmpty(value) && !IsEncrypted(value))
                        {
                            try
                            {
                                rowDict[field] = Encrypt(value);
                            }
                            catch (CryptographicException ex)
                            {
                                throw new AlgorithmIncapibilitiesException("Encryption failed due to incompatible algorithm or key", ex);
                            }
                        }
                    }
                }
                processedRows.Add(rowDict);
            }
        }
        catch (Exception ex)
        {
            throw new TransactionFailureException("An unexpected transaction failure occurred while transferring data", ex);
        }
        
        // insert each row from processedRows to the target table
        try
        {
            foreach (var row in processedRows)
            {
                // extract row to the dictionary
                var dict = row as IDictionary<string, object>;
                if (dict == null || dict.Count == 0)
                    continue;

                var col = string.Join(", ", dict.Keys);
                var parameters = string.Join(", ", dict.Keys.Select(c => "@" + c));
                var insertQuery = $"INSERT INTO {targetTable} ({col}) VALUES ({parameters})";
                try
                {
                    await targetConn.ExecuteAsync(insertQuery, dict);
                }
                catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                {
                    throw new DataIntegrityViolationException("Data integrity violation occured while insertin data into the target table", ex);
                }
            }
        }
        catch (Exception ex)
        {
            throw new TransactionFailureException("An unexpected transaction failure occurred while transferring data", ex);
        }
    }

    // Backup table
    public async Task BackupTable(string sourceConnection, string sourceTable, string schema = "public", int maxAllowedRows = 100000)
    {
        // Validate connection string
        ValidateConnectionString(sourceConnection);
        using var sourceConn = new NpgsqlConnection(sourceConnection);

        // Check row count first to simulate large data volume handling
        var countQuery = $"SELECT COUNT(*) FROM \"public\".\"{sourceTable}\"";
        int rowCount = await sourceConn.ExecuteScalarAsync<int>(countQuery);

        if (rowCount > maxAllowedRows)
        {
            throw new LargeDataVolumeException(
                $"The table '{sourceTable}' has {rowCount} rows which exceeds the maximum allowed {maxAllowedRows} for a single transfer operation.");
        }

        try
        {
            string backupTable = sourceTable + "_backup";

            // Check if backup database exist
            bool exists = await CheckTable(sourceConn, backupTable);
            Console.WriteLine($"The table {sourceTable}: {exists}"); // debug

            // Queries
            string query = $@"CREATE TABLE IF NOT EXISTS ""{schema}"".""{backupTable}"" AS TABLE ""{schema}"".""{sourceTable}"" WITH NO DATA;";
            string copyQuery = $@"INSERT INTO ""{schema}"".""{backupTable}"" SELECT * FROM ""{schema}"".""{sourceTable}"";";
            string PKQuery = $@"ALTER TABLE ""{schema}"".""{backupTable}"" ADD PRIMARY KEY (id);";

            if (!exists)
            {
                Console.WriteLine($"The table {backupTable} doesn't exist. Creating...");
                // create table
                await sourceConn.ExecuteAsync(query);

                // Inserting data to table
                Console.WriteLine($"The table {backupTable} exists. Updating...");
                try
                {
                    await sourceConn.ExecuteAsync(copyQuery);
                }
                catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                {
                    throw new DataIntegrityViolationException("Data integrity violation occured while insertin data into the target table", ex);
                }

                // Creating primary key for backup table
                Console.WriteLine($"The data has been inserted to the table: {backupTable}. Creating Primary Key...");
                try
                {
                    await sourceConn.ExecuteAsync(PKQuery);
                }
                catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                {
                    throw new DataIntegrityViolationException("Data integrity violation occured while insertin data into the target table", ex);
                }
                Console.WriteLine($"The backup table for {sourceTable} has been successfully created");
            }

            // Inserting data to table
            Console.WriteLine($"The table {backupTable} exists. Updating...");
            try
            {
                await sourceConn.ExecuteAsync(copyQuery);
            }
            catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
            {
                throw new DataIntegrityViolationException("Data integrity violation occured while insertin data into the target table", ex);
            }
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01")
        {
            throw new InvalidTableException($"The table {sourceTable} does not exist in the schema {schema}.", ex);
        }
    }

    // Tokenize table with selected columns
    public async Task TokenizeTable(string sourceConnection, string sourceTable, List<string> columns, int maxAllowedRows = 100000)
    {
        // Validate connection string
        ValidateConnectionString(sourceConnection);

        // Ensure the "id" column is always present for update reference.
        if (!columns.Any(c => string.Equals(c, "id", StringComparison.OrdinalIgnoreCase)))
        {
            columns.Add("id");
        }

        using var sourceConn = new NpgsqlConnection(sourceConnection);

        // Check row count first to simulate large data volume handling
        var countQuery = $"SELECT COUNT(*) FROM \"public\".\"{sourceTable}\"";
        int rowCount = await sourceConn.ExecuteScalarAsync<int>(countQuery);

        if (rowCount > maxAllowedRows)
        {
            throw new LargeDataVolumeException(
                $"The table '{sourceTable}' has {rowCount} rows which exceeds the maximum allowed {maxAllowedRows} for a single transfer operation.");
        }

        var selectedColumns = string.Join(", ", columns);
        Console.WriteLine($"The selected columns are {selectedColumns}");

        var selectQuery = $"SELECT {selectedColumns} FROM {sourceTable}";
        try
        {
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
                            try
                            {
                                userDict[field] = Encrypt(curr);
                            }
                            catch (CryptographicException ex)
                            {
                                throw new AlgorithmIncapibilitiesException("Encryption failed due to incompatible algorithm or key", ex);
                            }
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
                catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                {
                    throw new DataIntegrityViolationException("Data integrity violation occured while insertin data into the target table", ex);
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState == "42703")
        {
            throw new InvalidColumnException("The selected column does not exist", ex);
        }
    }

    // Detokenize whole table
    public async Task DetokenizeTable(string sourceConnection, string sourceTable, List<string> columns, int maxAllowedRows = 100000)
    {
        // Validate connection string
        ValidateConnectionString(sourceConnection);

        if (!columns.Any(c => string.Equals(c, "id", StringComparison.OrdinalIgnoreCase)))
        {
            columns.Add("id");
        }

        using var sourceConn = new NpgsqlConnection(sourceConnection);

        // Check row count first to simulate large data volume handling
        var countQuery = $"SELECT COUNT(*) FROM \"public\".\"{sourceTable}\"";
        int rowCount = await sourceConn.ExecuteScalarAsync<int>(countQuery);

        if (rowCount > maxAllowedRows)
        {
            throw new LargeDataVolumeException(
                $"The table '{sourceTable}' has {rowCount} rows which exceeds the maximum allowed {maxAllowedRows} for a single transfer operation.");
        }

        var selectedColumns = string.Join(", ", columns);
        Console.WriteLine($"The selected columns are {selectedColumns}");

        var selectQuery = $"SELECT {selectedColumns} FROM {sourceTable}";

        try
        {
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
                            try
                            {
                                userDict[field] = Decrypt(curr);
                            }
                            catch (CryptographicException ex)
                            {
                                throw new AlgorithmIncapibilitiesException("Encryption failed due to incompatible algorithm or key", ex);
                            }
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
                catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                {
                    throw new DataIntegrityViolationException("Data integrity violation occured while insertin data into the target table", ex);
                }
            }
        }
        catch (Exception ex)
        {
            throw new InvalidColumnException("The selected column does not exist.", ex);
        }
    }

}
