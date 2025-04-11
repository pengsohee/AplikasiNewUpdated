using AplikasiNew.Exceptions;
using AplikasiNew.Models.Errors;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;

namespace AplikasiNew.Middleware
{
    public class ProblemDetailsMiddleware(RequestDelegate next, ILogger<ProblemDetails>? logger)
    {
        private readonly RequestDelegate _next = next;
        private readonly ILogger<ProblemDetails> _logger = logger;

        private async Task HandleProblem(HttpContext context, Exception ex, string title, string type, int statusCode, string errorCode)
        {
            _logger.LogError(ex, title);

            var problemDetails = new CustomProblemDetails
            {
                Title = title,
                Detail = ex.Message,
                Status = statusCode,
                Type = type,
                ErrorCode = errorCode,
                Instance = context.Request.Path
            };

            context.Response.StatusCode = statusCode;
            context.Response.ContentType = "application/problem+json";
            await context.Response.WriteAsJsonAsync(problemDetails);
        }

        public async Task Invoke(HttpContext context)
        {
            try
            {
                await _next(context);
            }
            catch (InvalidConnectionStringException ex)
            {
                await HandleProblem(context, ex, "Invalid connection string", "about:blank", 400, "DB_CONN_001");
            }
            catch (DatabaseAuthException ex)
            {
                await HandleProblem(context, ex, "Authentication failed", "about:blank", 400, "DB_AUTH_001");
            }
            catch (DatabaseNetworkException ex)
            {
                await HandleProblem(context, ex, "Database unreachable", "about:blank", 400, "DB_NET_001");
            }
            catch (InvalidTableException ex)
            {
                await HandleProblem(context, ex, "Invalid table", "about:blank", 400, "DB_TBL_404");
            }
            catch (SchemaMismatchException ex)
            {
                await HandleProblem(context, ex, "Schema Mismatch Detected", "about:blank", 400, "DB_SCHEMA_001");
            }
            catch (DataIntegrityViolationException ex)
            {
                await HandleProblem(context, ex, "Data integrity violation", "about:blank", 400, "DB_INTEGRITY_001");
            }
            catch (LargeDataVolumeException ex)
            {
                await HandleProblem(context, ex, "Large data volume", "about:blank", 400, "DB_VOL_001");
            }
            catch (AlgorithmIncapibilitiesException ex)
            {
                await HandleProblem(context, ex, "Algorithm mismatch detected", "about:blank", 400, "ENC_001");
            }
            catch (InvalidColumnException ex)
            {
                await HandleProblem(context, ex, "Invalid column", "about:blank", 400, "DB_COL_404");
            }
            catch (KeyManagementException ex)
            {
                await HandleProblem(context, ex, "There is a problem retrieving the key", "about:blank", 400, "KEY_ERR_001");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "An unexpected error occurred");

                var problemDetails = new CustomProblemDetails
                {
                    Title = "An unexpected error occurred",
                    Detail = "An internal server error occurred. Please contact support.",
                    Status = StatusCodes.Status500InternalServerError,
                    Type = "https://example.com/problems/internal-error",
                    ErrorCode = "UNEXPECTED_500",
                    Instance = context.Request.Path
                };


                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                context.Response.ContentType = "application/problem+json";
                await context.Response.WriteAsJsonAsync(problemDetails);
            }
        }
    }
}
