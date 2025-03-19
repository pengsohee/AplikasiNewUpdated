using AplikasiNew.Services;
using Microsoft.AspNetCore.Mvc;

namespace AplikasiNew.Controllers;

[Route("api/[controller]")]
[ApiController]
public class DataController : ControllerBase
{
    private readonly DataService _dataService;

    public DataController(DataService dataService)
    {
        _dataService = dataService;
    }

    public class SubmitRequest
    {
        public string TokenGroup { get; set; }
        public string Source { get; set; }
    }

    [HttpPost("submit")]
    public async Task<IActionResult> Submit([FromBody] SubmitRequest request)
    {
        if (string.IsNullOrEmpty(request.TokenGroup) || string.IsNullOrEmpty(request.Source))
        {
            return BadRequest(new { error = "Token group and source are required" });
        }

        if (request.Source != "users" && request.Source != "nosecurity_db")
        {
            return BadRequest(new { error = "Invalid source provided" });
        }

        await _dataService.TransferData(request.Source);
        return Ok(new { message = "Data successfully transferred!" });
    }

    [HttpGet("get-data")]
    public async Task<IActionResult> GetData()
    {
        var data = await _dataService.GetData();
        return Ok(data);
    }

    [HttpPost("detokenize")]
    public async Task<IActionResult> Detokenize()
    {
        var data = await _dataService.DetokenizeData();
        return Ok(data);
    }
}
