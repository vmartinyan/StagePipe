namespace StagePipe.Web.Models;

public sealed class SyncRunState
{
    public string SelectedServiceKey { get; set; } = string.Empty;
    public string? SuccessMessage { get; set; }
    public string? ErrorMessage { get; set; }
}
