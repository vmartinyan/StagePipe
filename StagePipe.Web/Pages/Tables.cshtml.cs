using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using StagePipe.Web.Models;
using StagePipe.Web.Services;

namespace StagePipe.Web.Pages;

public sealed class TablesModel : PageModel
{
    private readonly IDatabaseBrowserService _databaseBrowserService;

    public TablesModel(IDatabaseBrowserService databaseBrowserService)
    {
        _databaseBrowserService = databaseBrowserService;
    }

    [BindProperty(SupportsGet = true)]
    public string Db { get; set; } = "Staging";

    [BindProperty(SupportsGet = true)]
    public string Table { get; set; } = string.Empty;

    [BindProperty(SupportsGet = true)]
    public int Limit { get; set; } = 100;

    public BrowsePageState Browse { get; private set; } = new();

    public async Task OnGetAsync(CancellationToken cancellationToken)
    {
        Browse = await _databaseBrowserService.LoadAsync(Db, Table, Limit, cancellationToken);

        Db = Browse.SelectedDb;
        Table = Browse.SelectedTable;
        Limit = Browse.Limit;
    }
}
