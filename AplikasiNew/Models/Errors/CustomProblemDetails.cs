using Microsoft.AspNetCore.Mvc;

namespace AplikasiNew.Models.Errors
{
    public class CustomProblemDetails : ProblemDetails
    {
        public string ErrorCode { get; set; }
    }
}
