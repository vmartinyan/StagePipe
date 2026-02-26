namespace StagePipe.Web.Services.Sync;

public interface ISyncTaskService
{
    IReadOnlyList<SyncTaskDefinition> GetAvailableTasks();
    string NormalizeTaskKey(string? taskKey);
    Task<SyncTaskResult> RunAsync(string taskKey, CancellationToken cancellationToken);
}
