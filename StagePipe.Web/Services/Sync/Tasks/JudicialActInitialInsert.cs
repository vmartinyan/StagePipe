using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class JudicialActInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public JudicialActInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "JudicialActInitialInsert";
    public string Title => "Judicial act initial insert";
    public string Description => "Load judicial acts from Production and insert into Staging judicial_act with mapped IDs.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/JudicialActInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var targetColumns = await ResolveTargetColumnsAsync(stagingConnection);

        var courtCaseByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `court_case`;", "id_api");
        var judgeByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `judge`;", "id_api");

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `judicial_act`;");

        var mappedRows = sourceRows
            .Select(row =>
            {
                var idApi = GetValue(row, "id_api")?.ToString()?.Trim();
                var courtCaseApiId = GetValue(row, "court_case_id_api")?.ToString()?.Trim();
                var judgeApiId = GetValue(row, "judge_id_api")?.ToString()?.Trim();
                var subtypeSlug = GetValue(row, "subtype_slug")?.ToString()?.Trim();

                var courtCaseId = courtCaseApiId is not null && courtCaseByApiId.TryGetValue(courtCaseApiId, out var mappedCourtCaseId)
                    ? mappedCourtCaseId
                    : null;

                var judgeId = judgeApiId is not null && judgeByApiId.TryGetValue(judgeApiId, out var mappedJudgeId)
                    ? mappedJudgeId
                    : null;

                var type = GetValue(row, "type")?.ToString()?.Trim();
                var typeName = GetValue(row, "type_name")?.ToString()?.Trim();
                var isPublic = GetValue(row, "is_public");
                var publishedAt = GetValue(row, "published_at");
                var status = GetValue(row, "status")?.ToString()?.Trim();

                return new
                {
                    idApi,
                    courtCaseId,
                    judgeId,
                    type,
                    typeName,
                    subtypeSlug,
                    isPublic,
                    publishedAt,
                    status
                };
            })
            .ToList();

        var insertRows = mappedRows
            .Select(row =>
            {
                var values = new Dictionary<string, object?>
                {
                    [targetColumns.CourtCaseIdColumn] = row.courtCaseId,
                    [targetColumns.JudgeIdColumn] = row.judgeId
                };

                if (targetColumns.IdApiColumn is not null)
                {
                    values[targetColumns.IdApiColumn] = row.idApi;
                }

                if (targetColumns.TypeColumn is not null)
                {
                    values[targetColumns.TypeColumn] = row.type;
                }

                if (targetColumns.TypeNameColumn is not null)
                {
                    values[targetColumns.TypeNameColumn] = row.typeName;
                }

                if (targetColumns.SubtypeSlugColumn is not null)
                {
                    values[targetColumns.SubtypeSlugColumn] = row.subtypeSlug;
                }

                if (targetColumns.IsPublicColumn is not null)
                {
                    values[targetColumns.IsPublicColumn] = row.isPublic;
                }

                if (targetColumns.PublishedAtColumn is not null)
                {
                    values[targetColumns.PublishedAtColumn] = row.publishedAt;
                }

                if (targetColumns.StatusColumn is not null)
                {
                    values[targetColumns.StatusColumn] = row.status;
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
                $"INSERT INTO `judicial_act` ({columnList}) VALUES ({valuesList});",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string CourtCaseIdColumn, string JudgeIdColumn, string? IdApiColumn, string? TypeColumn, string? TypeNameColumn, string? SubtypeSlugColumn, string? IsPublicColumn, string? PublishedAtColumn, string? StatusColumn)> ResolveTargetColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'judicial_act';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var courtCaseIdColumn = new[] { "court_case_id", "courtcase_id", "case_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(courtCaseIdColumn))
        {
            throw new InvalidOperationException("Could not resolve court_case reference column in judicial_act.");
        }

        var judgeIdColumn = new[] { "judge_id", "member_id", "user_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(judgeIdColumn))
        {
            throw new InvalidOperationException("Could not resolve judge reference column in judicial_act.");
        }

        var idApiColumn = new[] { "id_api", "api_id" }.FirstOrDefault(columns.Contains);
        var typeColumn = new[] { "type", "act_type", "kind" }.FirstOrDefault(columns.Contains);
        var typeNameColumn = new[] { "type_name", "name", "title" }.FirstOrDefault(columns.Contains);
        var subtypeSlugColumn = new[] { "subtype_slug", "slug" }.FirstOrDefault(columns.Contains);
        var isPublicColumn = new[] { "is_public", "public", "is_visible" }.FirstOrDefault(columns.Contains);
        var publishedAtColumn = new[] { "published_at", "publish_at", "created_at" }.FirstOrDefault(columns.Contains);
        var statusColumn = new[] { "status", "state" }.FirstOrDefault(columns.Contains);

        return (courtCaseIdColumn, judgeIdColumn, idApiColumn, typeColumn, typeNameColumn, subtypeSlugColumn, isPublicColumn, publishedAtColumn, statusColumn);
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