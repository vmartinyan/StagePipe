using Dapper;
using MySqlConnector;
using System.Data;
using System.Globalization;

namespace StagePipe.Web.Services.Sync;

public sealed class CourtCaseUpdate : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public CourtCaseUpdate(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "CourtCaseUpdate";
    public string Title => "Court case update";
    public string Description => "Update court_case with code, parent_id, counter_claim_id, is_collegial, appealed_act_id, mode, is_public and statistical_classifier from Production API.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/ProductionApiCourtCases.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var courtCaseByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `court_case`;", "id_api");
        var judicialActByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `judicial_act`;", "id_api");

        var updateCount = 0;

        foreach (var row in sourceRows)
        {
            var idApi = GetString(row, "id_api");

            if (idApi is null || !courtCaseByApiId.TryGetValue(idApi, out var courtCaseId))
            {
                continue;
            }

            var parentApiId = GetString(row, "parent_id_api");
            long? parentId = parentApiId is not null && courtCaseByApiId.TryGetValue(parentApiId, out var mappedParentId)
                ? mappedParentId
                : null;

            var counterClaimApiId = GetString(row, "counter_claim_id_api");
            long? counterClaimId = counterClaimApiId is not null && courtCaseByApiId.TryGetValue(counterClaimApiId, out var mappedCounterClaimId)
                ? mappedCounterClaimId
                : null;

            var appealApiId = GetString(row, "appeal_id_api");
            long? appealedActId = appealApiId is not null && judicialActByApiId.TryGetValue(appealApiId, out var mappedAppealedActId)
                ? mappedAppealedActId
                : null;

            var parameters = new DynamicParameters();
            parameters.Add("id", courtCaseId, DbType.Int64);
            parameters.Add("code", GetString(row, "code"), DbType.String);
            parameters.Add("parent_id", parentId, DbType.Int64);
            parameters.Add("counter_claim_id", counterClaimId, DbType.Int64);
            parameters.Add("is_collegial", GetBoolAsInt(row, "is_collegial"), DbType.Int32);
            parameters.Add("appealed_act_id", appealedActId, DbType.Int64);
            parameters.Add("mode", GetString(row, "mode"), DbType.String);
            parameters.Add("is_public", GetBoolAsInt(row, "is_public"), DbType.Int32);
            parameters.Add("statistical_classifier", GetString(row, "statistical_classifier"), DbType.String);

            await stagingConnection.ExecuteAsync(
                @"UPDATE `court_case`
                  SET `code` = @code,
                      `parent_id` = @parent_id,
                      `counter_claim_id` = @counter_claim_id,
                      `is_collegial` = @is_collegial,
                      `appealed_act_id` = @appealed_act_id,
                      `mode` = @mode,
                      `is_public` = @is_public,
                      `statistical_classifier` = @statistical_classifier
                  WHERE `id` = @id;",
                parameters,
                commandTimeout: 300);

            updateCount++;
        }

        cancellationToken.ThrowIfCancellationRequested();
        return updateCount;
    }

    private static async Task<Dictionary<string, long>> LoadLookupAsync(MySqlConnection connection, string query, string keyColumn)
    {
        var rows = (await connection.QueryAsync(
                query,
                commandTimeout: 120))
            .OfType<IDictionary<string, object>>()
            .ToList();

        return rows
            .Select(row =>
            {
                var idValue = GetValue(row, "id");
                var keyValue = GetValue(row, keyColumn)?.ToString()?.Trim();
                long? id = idValue is not null
                    ? Convert.ToInt64(idValue, CultureInfo.InvariantCulture)
                    : null;

                return new { id, key = keyValue };
            })
            .Where(x => x.id.HasValue && !string.IsNullOrWhiteSpace(x.key))
            .GroupBy(x => x.key!, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(
                group => group.Key,
                group => group.First().id!.Value,
                StringComparer.OrdinalIgnoreCase);
    }

    private static string? GetString(IDictionary<string, object> row, string key)
    {
        var value = GetValue(row, key);
        return value is not null ? Convert.ToString(value, CultureInfo.InvariantCulture)?.Trim() : null;
    }

    private static int? GetBoolAsInt(IDictionary<string, object> row, string key)
    {
        var value = GetValue(row, key);
        if (value is null) return null;
        if (value is bool b) return b ? 1 : 0;
        if (value is ulong u) return u != 0 ? 1 : 0;
        var s = Convert.ToString(value, CultureInfo.InvariantCulture);
        if (string.Equals(s, "true", StringComparison.OrdinalIgnoreCase)) return 1;
        if (string.Equals(s, "false", StringComparison.OrdinalIgnoreCase)) return 0;
        if (int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i)) return i != 0 ? 1 : 0;
        return null;
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
