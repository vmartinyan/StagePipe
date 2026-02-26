using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using StagePipe.Web.Models;
using StagePipe.Web.Services.Sync;

namespace StagePipe.Web.Pages;

public sealed class SyncModel : PageModel
{
    private readonly ISyncTaskService _syncTaskService;

    public SyncModel(ISyncTaskService syncTaskService)
    {
        _syncTaskService = syncTaskService;
    }

    [BindProperty(SupportsGet = true)]
    public string SyncService { get; set; } = string.Empty;

    public SyncRunState Sync { get; private set; } = new();
    public IReadOnlyList<SyncTaskDefinition> AvailableSyncServices { get; private set; } = [];

    public void OnGet()
    {
        LoadServices();
    }

    public async Task OnPostRunAsync(CancellationToken cancellationToken)
    {
        LoadServices();

        SyncService = _syncTaskService.NormalizeTaskKey(SyncService);
        var result = await _syncTaskService.RunAsync(SyncService, cancellationToken);

        Sync.SelectedServiceKey = SyncService;
        Sync.SuccessMessage = result.SuccessMessage;
        Sync.ErrorMessage = result.ErrorMessage;
    }

    private void LoadServices()
    {
        AvailableSyncServices = _syncTaskService.GetAvailableTasks();
        SyncService = _syncTaskService.NormalizeTaskKey(SyncService);

        Sync = new SyncRunState
        {
            SelectedServiceKey = SyncService
        };
    }
}
