using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class JudicialActSubTypeInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public JudicialActSubTypeInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "JudicialActSubTypeInitialInsert";
    public string Title => "Judicial act subtype initial insert";
    public string Description => "Load judicial act subtypes from Production and insert into Staging judicial_act_subtype.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/JudicialActSubTypeInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var targetColumns = await ResolveTargetColumnsAsync(stagingConnection);

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `judicial_act_subtype`;");

        var insertRows = sourceRows
            .Select(row => new
            {
                name = GetValue(row, "name")?.ToString()?.Trim(),
                slug = GetValue(row, "slug")?.ToString()?.Trim()
            })
            .Where(x => !string.IsNullOrWhiteSpace(x.name) || !string.IsNullOrWhiteSpace(x.slug))
            .GroupBy(x => $"{x.name ?? string.Empty}|{x.slug ?? string.Empty}", StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .Select(row => new Dictionary<string, object?>
            {
                [targetColumns.NameColumn] = row.name,
                [targetColumns.SlugColumn] = row.slug
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            var columns = insertRows[0].Keys.ToList();
            var columnList = string.Join(", ", columns.Select(column => $"`{column}`"));
            var valuesList = string.Join(", ", columns.Select(column => $"@{column}"));

            await stagingConnection.ExecuteAsync(
                $"INSERT INTO `judicial_act_subtype` ({columnList}) VALUES ({valuesList});",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string NameColumn, string SlugColumn)> ResolveTargetColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                                        AND TABLE_NAME = 'judicial_act_subtype';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var nameColumn = new[] { "name", "title", "label" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(nameColumn))
        {
            throw new InvalidOperationException("Could not resolve name column in judicial_act_subtype.");
        }

        var slugColumn = new[] { "slug", "code", "key" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(slugColumn))
        {
            throw new InvalidOperationException("Could not resolve slug column in judicial_act_subtype.");
        }

        return (nameColumn, slugColumn);
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