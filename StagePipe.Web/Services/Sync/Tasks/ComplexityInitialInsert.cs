using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class ComplexityInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public ComplexityInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "ComplexityInitialInsert";
    public string Title => "Complexity initial insert";
    public string Description => "Load complexity from ProductionAuth and insert into Staging complexity with mapped court_case_id by UUID.";
    public string SourceDatabase => "ProductionAuth";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/ComplexityInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var courtCaseByUuid = await LoadLookupAsync(stagingConnection, "SELECT id, uuid FROM `court_case`;", "uuid");

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `complexity`;");

        var insertRows = sourceRows
            .Select(row =>
            {
                var courtCaseUuid = GetValue(row, "court_case_uuid")?.ToString()?.Trim();
                if (string.IsNullOrWhiteSpace(courtCaseUuid))
                {
                    throw new InvalidOperationException("Complexity sync failed: source row has empty court_case_uuid.");
                }

                if (!courtCaseByUuid.TryGetValue(courtCaseUuid, out var courtCaseId))
                {
                    throw new InvalidOperationException($"Complexity sync failed: court_case_uuid '{courtCaseUuid}' is missing in Staging court_case table.");
                }

                return new
                {
                    court_case_id = courtCaseId,
                    type = GetValue(row, "type"),
                    coefficient = GetValue(row, "coefficient"),
                    modified_at = GetValue(row, "modified_at")
                };
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            await stagingConnection.ExecuteAsync(
                @"INSERT INTO `complexity` (`court_case_id`, `type`, `coefficient`, `modified_at`)
                  VALUES (@court_case_id, @type, @coefficient, @modified_at);",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<Dictionary<string, object>> LoadLookupAsync(MySqlConnection stagingConnection, string query, string keyColumn)
    {
        var rows = (await stagingConnection.QueryAsync(
                query,
                commandTimeout: 120))
            .OfType<IDictionary<string, object>>()
            .ToList();

        return rows
            .Select(row => new
            {
                id = GetValue(row, "id"),
                key = GetValue(row, keyColumn)?.ToString()?.Trim()
            })
            .Where(x => x.id is not null && !string.IsNullOrWhiteSpace(x.key))
            .GroupBy(x => x.key!, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First().id ?? throw new InvalidOperationException("Lookup row has null id."), StringComparer.OrdinalIgnoreCase);
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