using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class JudicialActHistoryInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public JudicialActHistoryInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "JudicialActHistoryInitialInsert";
    public string Title => "Judicial act history initial insert";
    public string Description => "Load judicial act history from Production and insert into Staging judicial_act_history with mapped judicial_act_id.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/JudicialActHistoryInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var targetColumns = await ResolveTargetColumnsAsync(stagingConnection);
        var judicialActByApiIdAndType = await LoadJudicialActLookupAsync(stagingConnection);

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `judicial_act_history`;");

        var mappedRows = sourceRows
            .Select(row =>
            {
                var judicialActIdApi = GetValue(row, "judicial_act_id_api")?.ToString()?.Trim();
                var type = GetValue(row, "type")?.ToString()?.Trim();

                var key = BuildKey(judicialActIdApi, type);
                var judicialActId = key is not null && judicialActByApiIdAndType.TryGetValue(key, out var mappedJudicialActId)
                    ? mappedJudicialActId
                    : null;

                var status = GetValue(row, "status")?.ToString()?.Trim();
                var startedAt = GetValue(row, "started_at");
                var endedAt = GetValue(row, "ended_at");
                var note = GetValue(row, "note")?.ToString();

                return new
                {
                    judicialActId,
                    type,
                    status,
                    startedAt,
                    endedAt,
                    note
                };
            })
            .ToList();

        var insertRows = mappedRows
            .Select(row =>
            {
                var values = new Dictionary<string, object?>
                {
                    [targetColumns.JudicialActIdColumn] = row.judicialActId
                };

                if (targetColumns.TypeColumn is not null)
                {
                    values[targetColumns.TypeColumn] = row.type;
                }

                if (targetColumns.StatusColumn is not null)
                {
                    values[targetColumns.StatusColumn] = row.status;
                }

                if (targetColumns.StartedAtColumn is not null)
                {
                    values[targetColumns.StartedAtColumn] = row.startedAt;
                }

                if (targetColumns.EndedAtColumn is not null)
                {
                    values[targetColumns.EndedAtColumn] = row.endedAt;
                }

                if (targetColumns.NoteColumn is not null)
                {
                    values[targetColumns.NoteColumn] = row.note;
                }

                return values;
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            var columns = insertRows[0].Keys.ToList();
            var columnList = string.Join(", ", columns.Select(column => $"`{column}`"));
            var valuesList = string.Join(", ", columns.Select(column => $"@{column}"));

            await stagingConnection.ExecuteAsync(
                $"INSERT INTO `judicial_act_history` ({columnList}) VALUES ({valuesList});",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string JudicialActIdColumn, string? TypeColumn, string? StatusColumn, string? StartedAtColumn, string? EndedAtColumn, string? NoteColumn)> ResolveTargetColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'judicial_act_history';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var judicialActIdColumn = new[] { "judicial_act_id", "judical_act_id", "act_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(judicialActIdColumn))
        {
            throw new InvalidOperationException("Could not resolve judicial_act reference column in judicial_act_history.");
        }

        var typeColumn = new[] { "type", "act_type", "kind" }.FirstOrDefault(columns.Contains);
        var statusColumn = new[] { "status", "state" }.FirstOrDefault(columns.Contains);
        var startedAtColumn = new[] { "started_at", "start_at", "valid_from" }.FirstOrDefault(columns.Contains);
        var endedAtColumn = new[] { "ended_at", "end_at", "valid_to" }.FirstOrDefault(columns.Contains);
        var noteColumn = new[] { "note", "description", "comment" }.FirstOrDefault(columns.Contains);

        return (judicialActIdColumn, typeColumn, statusColumn, startedAtColumn, endedAtColumn, noteColumn);
    }

    private static async Task<Dictionary<string, object>> LoadJudicialActLookupAsync(MySqlConnection stagingConnection)
    {
        var rows = (await stagingConnection.QueryAsync(
                "SELECT id, id_api, type FROM `judicial_act`;",
                commandTimeout: 120))
            .OfType<IDictionary<string, object>>()
            .ToList();

        return rows
            .Select(row => new
            {
                id = GetValue(row, "id"),
                idApi = GetValue(row, "id_api")?.ToString()?.Trim(),
                type = GetValue(row, "type")?.ToString()?.Trim()
            })
            .Select(x => new
            {
                x.id,
                key = BuildKey(x.idApi, x.type)
            })
            .Where(x => x.id is not null && x.key is not null)
            .GroupBy(x => x.key!, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First().id ?? throw new InvalidOperationException("Lookup row has null id."), StringComparer.OrdinalIgnoreCase);
    }

    private static string? BuildKey(string? idApi, string? type)
    {
        if (string.IsNullOrWhiteSpace(idApi) || string.IsNullOrWhiteSpace(type))
        {
            return null;
        }

        return $"{idApi.Trim()}|{type.Trim()}";
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