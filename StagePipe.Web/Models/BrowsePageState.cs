namespace StagePipe.Web.Models;

public sealed class BrowsePageState
{
    public string SelectedDb { get; set; } = "Staging";
    public string SelectedTable { get; set; } = string.Empty;
    public int Limit { get; set; } = 100;
    public IReadOnlyList<string> Tables { get; set; } = [];
    public IReadOnlyList<IDictionary<string, object?>> Rows { get; set; } = [];
    public string? BrowseError { get; set; }

    public IReadOnlyList<string> Columns =>
        Rows.SelectMany(row => row.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();
}
