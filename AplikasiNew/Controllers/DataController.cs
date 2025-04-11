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
        public string SourceConnectionString { get; set; }
        public string SourceTable { get; set; }
        public List<string> Columns { get; set; }
    }

    public class BackupRequest
    {
        public string TokenGroup { get; set; }
        public string SourceConnectionString { get; set; }
        public string SourceTable { get; set; }
    }

    public class TransferTableRequest
    {
        public string SourceConnectionString { get; set; }
        public string TargetConnectionString { get; set; }
        public string SourceTable { get; set; }
        public string TargetTable { get; set; }
        public List<string> Columns { get; set; }
    }

    [HttpPost("backup-table")]
    public async Task<IActionResult> BackupTable([FromBody] BackupRequest request)
    {
        if (string.IsNullOrEmpty(request.TokenGroup) || string.IsNullOrEmpty(request.SourceTable))
        {
            return BadRequest(new { error = "Token group and source are required" });
        }

        await _dataService.BackupTable(request.SourceConnectionString, request.SourceTable);
        return Ok(new { message = "Backup table created" });
    }

    [HttpPost("transfer-database")]
    public async Task<IActionResult> TransferTable([FromBody] TransferTableRequest request)
    {
        if (request == null ||
            string.IsNullOrWhiteSpace(request.SourceConnectionString) ||
            string.IsNullOrWhiteSpace(request.TargetConnectionString) ||
            string.IsNullOrWhiteSpace(request.SourceTable) ||
            string.IsNullOrWhiteSpace(request.TargetTable))
        {
            return BadRequest("Invalid request parameters.");
        }

        await _dataService.TransferTable(request.SourceConnectionString, request.TargetConnectionString, request.SourceTable, request.TargetTable, request.Columns);
        return Ok(new { message = "The target table is up to date." });
    }

    [HttpPut("tokenize-table")]
    public async Task<IActionResult> TokenizeTable([FromBody] SubmitRequest request)
    {
        if (string.IsNullOrEmpty(request.TokenGroup) || string.IsNullOrEmpty(request.SourceTable) || request.Columns == null)
        {
            return BadRequest(new { error = "Token group and source are required" });
        }

        await _dataService.ProcessTableAsync(request.SourceConnectionString, request.SourceTable, request.Columns, isTokenized: false);
        return Ok(new { message = "Data successfully tokenized!" });
    }

    [HttpPut("detokenize-table")]
    public async Task<IActionResult> DetokenizeTable([FromBody] SubmitRequest request)
    {
        if (string.IsNullOrEmpty(request.TokenGroup) || string.IsNullOrEmpty(request.SourceTable))
        {
            return BadRequest(new { error = "Token group and source are required" });
        }

        await _dataService.ProcessTableAsync(request.SourceConnectionString,request.SourceTable, request.Columns, isTokenized: true);
        return Ok(new { message = "Data successfully detokenized!" });
    }

    [HttpGet("simulate-unhandled-error")]
    public IActionResult SimulateUnhandled()
    {
        throw new Exception("This is a simulated unhandled error.");
    }

    //[HttpPost("submit")]
    //public async Task<IActionResult> Submit([FromBody] SubmitRequest request)
    //{
    //    if (string.IsNullOrEmpty(request.TokenGroup) || string.IsNullOrEmpty(request.Source))
    //    {
    //        return BadRequest(new { error = "Token group and source are required" });
    //    }

    //    if (request.Source != "users" && request.Source != "nosecurity_db")
    //    {
    //        return BadRequest(new { error = "Invalid source provided" });
    //    }

    //    await _dataService.TransferData(request.Source);
    //    return Ok(new { message = "Data successfully transferred!" });
    //}

    //[HttpGet("get-data")]
    //public async Task<IActionResult> GetData()
    //{
    //    var data = await _dataService.GetData();
    //    return Ok(data);
    //}

    //[HttpPost("detokenize")]
    //public async Task<IActionResult> Detokenize()
    //{
    //    var data = await _dataService.DetokenizeData();
    //    return Ok(data);
    //}
}
