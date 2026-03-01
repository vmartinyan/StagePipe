using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class ParticipantTypeInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public ParticipantTypeInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "ParticipantTypeInitialInsert";
    public string Title => "Participant type initial insert";
    public string Description => "Load participant types from Production and insert into Staging participant_type.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/ParticipantTypeInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 120))
            .Cast<IDictionary<string, object>>()
            .ToList();

        var (idColumn, nameColumn) = await ResolveTargetColumnsAsync(stagingConnection);

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `participant_type`;");

        var insertRows = sourceRows
            .Select(row => new
            {
                identifier = GetValue(row, "identifier"),
                name = GetValue(row, "name")?.ToString()
            })
            .Where(x => x.identifier is not null && !string.IsNullOrWhiteSpace(x.name))
            .GroupBy(x => x.identifier)
            .Select(group => group.First())
            .ToList();

        if (insertRows.Count > 0)
        {
            await stagingConnection.ExecuteAsync(
                $@"INSERT INTO `participant_type` (`{idColumn}`, `{nameColumn}`)
                   VALUES (@identifier, @name);",
                insertRows,
                commandTimeout: 120);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string IdColumn, string NameColumn)> ResolveTargetColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'participant_type';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var idColumn = new[] { "identifier", "id", "id_api" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(idColumn))
        {
            throw new InvalidOperationException("Could not resolve ID column for participant_type. Expected one of: identifier, id, id_api.");
        }

        var nameColumn = new[] { "name", "title" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(nameColumn))
        {
            throw new InvalidOperationException("Could not resolve name column for participant_type. Expected one of: name, title.");
        }

        return (idColumn, nameColumn);
    }

    private static object? GetValue(IDictionary<string, object> row, string key)
    {
        if (row.TryGetValue(key, out var value) && value is not DBNull)
        {
            return value;
        }

        var matchedKey = row.Keys.FirstOrDefault(k => string.Equals(k, key, StringComparison.OrdinalIgnoreCase));
        if (matchedKey is not null && row.TryGetValue(matchedKey, out var matchedValue) && matchedValue is not DBNull)
        {
            return matchedValue;
        }

        return null;
    }
}
