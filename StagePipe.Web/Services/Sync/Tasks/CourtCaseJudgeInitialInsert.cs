using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class CourtCaseJudgeInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public CourtCaseJudgeInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "CourtCaseJudgeInitialInsert";
    public string Title => "Court case judge initial insert";
    public string Description => "Load court case judge rows from Production and insert into Staging court_case_judge with mapped IDs.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/CourtCaseJudgeInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var targetColumns = await ResolveTargetColumnsAsync(stagingConnection);

        var courtCaseByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `court_case`;", "id_api");
        var judgeByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `judge`;", "id_api");

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `court_case_judge`;");

        var mappedRows = sourceRows
            .Select(row =>
            {
                var courtCaseApiId = GetValue(row, "court_case_id_api")?.ToString()?.Trim();
                var judgeApiId = GetValue(row, "judge_id_api")?.ToString()?.Trim();

                var courtCaseId = courtCaseApiId is not null && courtCaseByApiId.TryGetValue(courtCaseApiId, out var mappedCourtCaseId)
                    ? mappedCourtCaseId
                    : null;

                var judgeId = judgeApiId is not null && judgeByApiId.TryGetValue(judgeApiId, out var mappedJudgeId)
                    ? mappedJudgeId
                    : null;

                var role = GetValue(row, "role")?.ToString()?.Trim();
                var joinedAt = GetValue(row, "joined_at");
                var leavedAt = GetValue(row, "leaved_at");

                return new
                {
                    courtCaseId,
                    judgeId,
                    role,
                    joinedAt,
                    leavedAt,
                    uniqueKey = $"{courtCaseApiId ?? string.Empty}|{judgeApiId ?? string.Empty}|{role ?? string.Empty}|{joinedAt?.ToString() ?? string.Empty}|{leavedAt?.ToString() ?? string.Empty}"
                };
            })
            .Where(x => x.courtCaseId is not null && x.judgeId is not null)
            .GroupBy(x => x.uniqueKey, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();

        var insertRows = mappedRows
            .Select(row =>
            {
                var values = new Dictionary<string, object?>
                {
                    [targetColumns.CourtCaseIdColumn] = row.courtCaseId,
                    [targetColumns.JudgeIdColumn] = row.judgeId
                };

                if (targetColumns.RoleColumn is not null)
                {
                    values[targetColumns.RoleColumn] = row.role;
                }

                if (targetColumns.JoinedAtColumn is not null)
                {
                    values[targetColumns.JoinedAtColumn] = row.joinedAt;
                }

                if (targetColumns.LeavedAtColumn is not null)
                {
                    values[targetColumns.LeavedAtColumn] = row.leavedAt;
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
                $"INSERT INTO `court_case_judge` ({columnList}) VALUES ({valuesList});",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string CourtCaseIdColumn, string JudgeIdColumn, string? RoleColumn, string? JoinedAtColumn, string? LeavedAtColumn)> ResolveTargetColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'court_case_judge';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var courtCaseIdColumn = new[] { "court_case_id", "courtcase_id", "case_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(courtCaseIdColumn))
        {
            throw new InvalidOperationException("Could not resolve court_case reference column in court_case_judge.");
        }

        var judgeIdColumn = new[] { "judge_id", "member_id", "user_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(judgeIdColumn))
        {
            throw new InvalidOperationException("Could not resolve judge reference column in court_case_judge.");
        }

        var roleColumn = new[] { "role", "judge_role", "type" }.FirstOrDefault(columns.Contains);
        var joinedAtColumn = new[] { "joined_at", "join_at", "started_at" }.FirstOrDefault(columns.Contains);
        var leavedAtColumn = new[] { "leaved_at", "left_at", "ended_at" }.FirstOrDefault(columns.Contains);

                return (courtCaseIdColumn, judgeIdColumn, roleColumn, joinedAtColumn, leavedAtColumn);
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