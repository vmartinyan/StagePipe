using StagePipe.Web.Models;

namespace StagePipe.Web.Services;

public interface IDatabaseBrowserService
{
    Task<BrowsePageState> LoadAsync(string selectedDb, string selectedTable, int limit, CancellationToken cancellationToken);
}
