﻿using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using AplikasiNew.Exceptions;
using AplikasiNew.Models;
using Dapper;
using DotNetEnv;
using Microsoft.AspNetCore.Components.Server;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.Extensions.Logging;
using Npgsql;
using static System.Runtime.InteropServices.JavaScript.JSType;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

namespace AplikasiNew.Services;

public class DataService
{
    private readonly IConfiguration _config;
    private readonly IEncryptionService _encryptionService;
    private readonly IValidationService _validationService;
    private readonly List<string> _encryptedFields;
    private readonly ILogger<DataService> _logger;
    public DataService(IConfiguration config, IEncryptionService encryptionService,  IValidationService validationService, ILogger<DataService> logger)
    {
        _config = config;
        _encryptionService = encryptionService;
        _validationService = validationService;
        _encryptedFields = _config.GetSection("EncryptionFields").Get<List<string>>() ?? new List<string>();
        _logger = logger;
    }
    #region Helper Method
    private (string query, DynamicParameters parameters) BuildUpsertQuery(string targetTable,List<IDictionary<string, object>> dataRows, List<string> pkColumns)
    {
        if (dataRows.Count == 0) return (string.Empty, null);

        // Get all column names from first row
        var allColumns = dataRows.First().Keys.ToList();

        // Build conflict target and update clause
        var quotedPkColumns = pkColumns.Select(c => $"\"{c}\"");
        var updateColumns = allColumns
            .Except(pkColumns)
            .Select(c => $"\"{c}\" = EXCLUDED.\"{c}\"");

        // Build parameterized query
        return ($@"
        INSERT INTO ""{targetTable}"" 
        ({string.Join(", ", allColumns.Select(c => $"\"{c}\""))})
        VALUES ({string.Join(", ", allColumns.Select(c => $"@{c}"))})
        ON CONFLICT ({string.Join(", ", quotedPkColumns)})
        DO UPDATE SET {string.Join(", ", updateColumns)}
        WHERE {string.Join(" AND ", pkColumns.Select(pk => $"\"{targetTable}\".\"{pk}\" = EXCLUDED.\"{pk}\""))}",
            new DynamicParameters());
    }

    private async Task ExecuteBulkUpsertAsync(NpgsqlConnection connection, string query,List<IDictionary<string, object>> dataRows, List<string> pkColumns)
    {
        if (string.IsNullOrEmpty(query)) return;

        try
        {
            await connection.ExecuteAsync(query, dataRows);
            _logger.LogInformation($"Upserted {dataRows.Count} rows successfully");
        }
        catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
        {
            throw new DataIntegrityViolationException("Data integrity violation during upsert operation", ex);
        }
    }
    private async Task<List<string>> GetPrimaryKeyColumnsAsync(NpgsqlConnection connection, string schema, string table)
    {
        var query = @"
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = @fullTableName::regclass
          AND i.indisprimary";

        var fullTableName = $"{schema}.{table}";
        return (await connection.QueryAsync<string>(query, new { fullTableName })).ToList();
    }
    private async Task InsertOrUpdateDataAsync(NpgsqlConnection connection, string schema, string sourceTable, string backupTable)
    {
        // Get all columns from the source table
        var columns = await GetTableColumnsAsync(connection, schema, sourceTable);
        var columnsToUpdate = columns.Where(c => c != "id").Select(c => $"\"{c}\" = EXCLUDED.\"{c}\"").ToList();

        // Build the ON CONFLICT SET clause
        string setClause = string.Join(", ", columnsToUpdate);

        string upsertQuery = $@"INSERT INTO ""{schema}"".""{backupTable}"" SELECT * FROM ""{schema}"".""{sourceTable}"" ON CONFLICT (id) DO UPDATE SET {setClause};";

        try
        {
            await connection.ExecuteAsync(upsertQuery);
        }
        catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
        {
            throw new DataIntegrityViolationException("Data integrity violation during upsert.", ex);
        }
    }

    private async Task<List<string>> GetTableColumnsAsync(NpgsqlConnection connection, string schema, string table)
    {
        var query = @"
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = @schema AND table_name = @table";
        var columns = await connection.QueryAsync<string>(query, new { schema, table });
        return columns.ToList();
    }

    private async Task<bool> CheckPrimaryKeyExistsAsync(NpgsqlConnection connection, string schema, string table)
    {
        var query = @"
        SELECT EXISTS (
            SELECT 1 
            FROM pg_constraint 
            WHERE conrelid = @fullTableName::regclass 
            AND contype = 'p'
        );";
        var fullTableName = $"{schema}.{table}";
        return await connection.ExecuteScalarAsync<bool>(query, new { fullTableName });
    } 
    #endregion

    #region Schema Handling
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

    #endregion

    #region Table Operations
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

    // Transfer data to other table (tokenization)
    public async Task TransferTable(string sourceConnection, string targetConnection, string sourceTable, string targetTable, List<string> columns, int maxAllowedRows = 100000)
    {
        // Validate connection string
        _validationService.ValidateConnectionString(sourceConnection);
        _validationService.ValidateConnectionString(targetConnection);

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
                        if (!string.IsNullOrEmpty(value) && !_encryptionService.IsEncrypted(value))
                        {
                            try
                            {
                                rowDict[field] = _encryptionService.Encrypt(value);
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
            var pkColumns = await GetPrimaryKeyColumnsAsync(targetConn, "public", targetTable);
            if (pkColumns.Count == 0)
                throw new InvalidOperationException($"Target table {targetTable} has no primary key");

            var dataRows = processedRows.Select(row => (IDictionary<string, object>)row).Where(dict => dict?.Count > 0).ToList();

            foreach (var row in dataRows)
            {
                if (!pkColumns.All(pk => row.ContainsKey(pk)))
                    throw new InvalidOperationException($"Missing primary key in row data: {string.Join(", ", pkColumns)}");
            }

            var (upsertQuery, parameters) = BuildUpsertQuery(targetTable, dataRows, pkColumns);
            await ExecuteBulkUpsertAsync(targetConn, upsertQuery, dataRows, pkColumns);

        }
        catch (Exception ex)
        {
            throw new TransactionFailureException("Transaction failure during data transfer", ex);
        }
    }

    // Backup table
    public async Task BackupTable(string sourceConnection, string sourceTable, string schema = "public", int maxAllowedRows = 100000)
    {
        // Validate connection string
        _validationService.ValidateConnectionString(sourceConnection);
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
            bool exists = await _validationService.CheckTable(sourceConn, backupTable);
            _logger.LogInformation($"The table {sourceTable}: {exists}"); // debug

            // Queries
            string createQuery = $@"CREATE TABLE IF NOT EXISTS ""{schema}"".""{backupTable}"" AS TABLE ""{schema}"".""{sourceTable}"" WITH NO DATA;";
            string pkQuery = $@"ALTER TABLE ""{schema}"".""{backupTable}"" ADD PRIMARY KEY (id);";

            if (!exists)
            {
                _logger.LogInformation($"Creating backup table {backupTable}...");
                await sourceConn.ExecuteAsync(createQuery);

                _logger.LogInformation($"Copying data to {backupTable}...");
                try
                {
                    await InsertOrUpdateDataAsync(sourceConn, schema, sourceTable, backupTable);
                }
                catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                {
                    throw new DataIntegrityViolationException("Data integrity violation occured while insertin data into the target table", ex);
                }

                // Creating primary key for backup table
                _logger.LogInformation($"Adding primary key to {backupTable}...");
                try
                {
                    await sourceConn.ExecuteAsync(pkQuery);
                }
                catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                {
                    throw new DataIntegrityViolationException("Failed to add primary key to backup table.", ex);
                }
                    _logger.LogInformation($"The backup table for {sourceTable} has been successfully created");
            }
            else
            {
                bool hasPk = await CheckPrimaryKeyExistsAsync(sourceConn, schema, backupTable);
                if (!hasPk)
                {
                    _logger.LogInformation($"Adding missing primary key to {backupTable}...");
                    try
                    {
                        await sourceConn.ExecuteAsync(pkQuery);
                    }
                    catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                    {
                        throw new DataIntegrityViolationException("Failed to add primary key to existing backup table.", ex);
                    }
                }
                _logger.LogInformation($"Updating existing data in {backupTable}...");
                try
                {
                    await InsertOrUpdateDataAsync(sourceConn, schema, sourceTable, backupTable);
                }
                catch (PostgresException ex) when (ex.SqlState.StartsWith("23"))
                {
                    throw new DataIntegrityViolationException("Data integrity violation occured while insertin data into the target table", ex);
                }
            }
            _logger.LogInformation($"Backup for {sourceTable} completed successfully.");
        }
        catch (PostgresException ex) when (ex.SqlState == "42P01")
        {
            throw new InvalidTableException($"The table {sourceTable} does not exist in the schema {schema}.", ex);
        }
    }

    // Tokenize and Detokenize
    public async Task ProcessTableAsync(string sourceConnection, string sourceTable, List<string> columns, bool isTokenized, int maxAllowedRows = 100000)
    {
        // Validate connection string
        _validationService.ValidateConnectionString(sourceConnection);

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
        _logger.LogInformation($"The selected columns are {selectedColumns}");
        var selectQuery = $"SELECT {selectedColumns} FROM {sourceTable}";

        try
        {
            var users = await sourceConn.QueryAsync<dynamic>(selectQuery);
            foreach (var row in users)
            {
                // dictionary
                var rowDict = row as IDictionary<string, object>;
                foreach (var field in _encryptedFields)
                {
                    // check if columns are available
                    if (columns.Contains(field, StringComparer.OrdinalIgnoreCase) && rowDict.ContainsKey(field))
                    {
                        var value = rowDict[field]?.ToString();
                        if (!string.IsNullOrEmpty(value))
                        {
                            try
                            {
                                rowDict[field] = isTokenized ? _encryptionService.Decrypt(value) : _encryptionService.Encrypt(value);
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
                    await sourceConn.ExecuteAsync(updateQuery, rowDict);
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

    #endregion
}
