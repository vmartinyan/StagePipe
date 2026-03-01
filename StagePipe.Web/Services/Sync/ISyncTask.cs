using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public interface ISyncTask
{
    string Key { get; }
    string Title { get; }
    string Description { get; }
    string SourceDatabase => "Production";

    Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken);
}
