using AplikasiNew.Exceptions;
using Dapper;
using Npgsql;

namespace AplikasiNew.Services
{
    public interface IValidationService
    {
        void ValidateConnectionString (string connectionString);
        Task<bool> CheckTable(NpgsqlConnection conn, string sourceTable, string schema = "public");
    }
    public class ValidationService : IValidationService
    {
        private readonly ILogger<ValidationService> _logger;
        public ValidationService(ILogger<ValidationService> logger)
        {
            _logger = logger;
        }
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

        public async Task<bool> CheckTable(NpgsqlConnection conn, string sourceTable, string schema = "public")
        {
            string query = @"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @schema AND table_name = @sourceTable";
            _logger.LogInformation($"schema: {schema}, sourceTable: {sourceTable}");
            long count = await conn.ExecuteScalarAsync<long>(query, new { schema, sourceTable });
            return count > 0;
        }

    }
}
