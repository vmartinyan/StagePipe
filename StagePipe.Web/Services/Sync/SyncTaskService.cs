using Microsoft.Extensions.Configuration;
using MySqlConnector;
using StagePipe.Web.Services.Connectivity;

namespace StagePipe.Web.Services.Sync;

public sealed class SyncTaskService : ISyncTaskService
{
    private readonly IConfiguration _configuration;
    private readonly ISshTunnelManager _sshTunnelManager;
    private readonly IReadOnlyDictionary<string, ISyncTask> _tasks;

    public SyncTaskService(IConfiguration configuration, ISshTunnelManager sshTunnelManager, IEnumerable<ISyncTask> tasks)
    {
        _configuration = configuration;
        _sshTunnelManager = sshTunnelManager;
        _tasks = tasks.ToDictionary(task => task.Key, StringComparer.OrdinalIgnoreCase);
    }

    public IReadOnlyList<SyncTaskDefinition> GetAvailableTasks()
    {
        return _tasks.Values
            .Select(task => new SyncTaskDefinition(task.Key, task.Title, task.Description))
            .OrderBy(task => task.Title, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public string NormalizeTaskKey(string? taskKey)
    {
        if (string.IsNullOrWhiteSpace(taskKey))
        {
            return _tasks.Keys.OrderBy(key => key, StringComparer.OrdinalIgnoreCase).FirstOrDefault() ?? string.Empty;
        }

        var matched = _tasks.Keys.FirstOrDefault(key => string.Equals(key, taskKey, StringComparison.OrdinalIgnoreCase));
        return matched ?? (_tasks.Keys.OrderBy(key => key, StringComparer.OrdinalIgnoreCase).FirstOrDefault() ?? string.Empty);
    }

    public async Task<SyncTaskResult> RunAsync(string taskKey, CancellationToken cancellationToken)
    {
        var normalizedKey = NormalizeTaskKey(taskKey);
        if (!_tasks.TryGetValue(normalizedKey, out var task))
        {
            return new SyncTaskResult { ErrorMessage = $"Sync service '{taskKey}' is not configured." };
        }

        var sourceDatabase = string.IsNullOrWhiteSpace(task.SourceDatabase) ? "Production" : task.SourceDatabase;
        var sourceConnectionString = _configuration.GetConnectionString(sourceDatabase);
        var stagingConnectionString = _configuration.GetConnectionString("Staging");

        if (string.IsNullOrWhiteSpace(sourceConnectionString) || string.IsNullOrWhiteSpace(stagingConnectionString))
        {
            return new SyncTaskResult { ErrorMessage = $"Missing {sourceDatabase} or Staging connection string." };
        }

        try
        {
            await _sshTunnelManager.EnsureForDatabaseAsync(sourceDatabase, cancellationToken);
            await _sshTunnelManager.EnsureForDatabaseAsync("Staging", cancellationToken);

            await using var sourceConnection = new MySqlConnection(sourceConnectionString);
            await using var stagingConnection = new MySqlConnection(stagingConnectionString);

            await sourceConnection.OpenAsync(cancellationToken);
            await stagingConnection.OpenAsync(cancellationToken);

            var affectedRows = await task.ExecuteAsync(sourceConnection, stagingConnection, cancellationToken);
            return new SyncTaskResult { SuccessMessage = $"Service '{task.Title}' executed successfully. Rows written: {affectedRows}." };
        }
        catch (Exception ex)
        {
            return new SyncTaskResult { ErrorMessage = BuildErrorChain(ex) };
        }
    }

    private static string BuildErrorChain(Exception ex)
    {
        var messages = new List<string>();
        var current = ex;

        while (current is not null)
        {
            messages.Add(current.Message);
            current = current.InnerException;
        }

        return string.Join(" | ", messages.Distinct(StringComparer.Ordinal));
    }
}
