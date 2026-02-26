namespace StagePipe.Web.Services.Connectivity;

public interface ISshTunnelManager
{
    Task EnsureForDatabaseAsync(string databaseName, CancellationToken cancellationToken);
}
