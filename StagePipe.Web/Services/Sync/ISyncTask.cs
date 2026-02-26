using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public interface ISyncTask
{
    string Key { get; }
    string Title { get; }
    string Description { get; }

    Task<int> ExecuteAsync(MySqlConnection productionConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken);
}
