using Dapper;
using MySqlConnector;
using System.Globalization;

namespace StagePipe.Web.Services.Sync;

public sealed class CourtCaseInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public CourtCaseInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "CourtCaseInitialInsert";
    public string Title => "Court case initial insert";
    public string Description => "Load initial court_case fields from Production API (uuid, instance_id, type, id_api).";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/ProductionApiCourtCases.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .Cast<IDictionary<string, object>>()
            .ToList();

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `court_case`;");

        var insertRows = sourceRows
            .Select(row => new
            {
                id_api = GetValue(row, "id_api"),
                uuid = GetValue(row, "uuid")?.ToString(),
                instance_id = GetValue(row, "instance_id") is { } instanceValue
                    && instanceValue is not DBNull
                    && int.TryParse(Convert.ToString(instanceValue, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInstanceId)
                    ? parsedInstanceId
                    : (int?)null,
                type = GetValue(row, "type")?.ToString(),
                unique_key = GetValue(row, "id_api") is { } idApiKeyValue
                    ? $"api:{Convert.ToString(idApiKeyValue, CultureInfo.InvariantCulture)}"
                    : (GetValue(row, "uuid") is { } uuidKeyValue && !string.IsNullOrWhiteSpace(uuidKeyValue?.ToString())
                        ? $"uuid:{uuidKeyValue.ToString()!.Trim().ToLowerInvariant()}"
                        : null)
            })
            .Where(x => !string.IsNullOrWhiteSpace(x.unique_key))
            .GroupBy(x => x.unique_key!, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var selected = group
                    .OrderByDescending(x => !string.IsNullOrWhiteSpace(x.uuid))
                    .ThenByDescending(x => x.instance_id.HasValue)
                    .First();

                return new
                {
                    selected.uuid,
                    selected.instance_id,
                    selected.type,
                    selected.id_api
                };
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            await stagingConnection.ExecuteAsync(
                @"INSERT IGNORE INTO `court_case` (`uuid`, `instance_id`, `type`, `id_api`)
                  VALUES (@uuid, @instance_id, @type, @id_api);",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
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
