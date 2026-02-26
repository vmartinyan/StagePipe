using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class CourtCaseStatisticsStatusesToStatusTask : ISyncTask
{
    public string Key => "CourtCaseStatisticsStatusesToStatus";
    public string Title => "CourtCaseStatisticsStatuses -> status";
    public string Description => "Truncate staging status table and reload Name->name, tag->slug.";

    public async Task<int> ExecuteAsync(MySqlConnection productionConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var sourceRows = (await productionConnection.QueryAsync(
                "SELECT Name, tag FROM CourtCaseStatisticsStatuses",
                commandTimeout: 120))
            .Cast<IDictionary<string, object>>()
            .ToList();

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `status`;");

        var insertRows = sourceRows
            .Select(row => new
            {
                name = row.TryGetValue("Name", out var nameValue) && nameValue is not DBNull ? nameValue?.ToString() : null,
                slug = row.TryGetValue("tag", out var tagValue) && tagValue is not DBNull ? tagValue?.ToString() : null
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            await stagingConnection.ExecuteAsync(
                "INSERT INTO `status` (`name`, `slug`) VALUES (@name, @slug);",
                insertRows,
                commandTimeout: 120);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }
}
