using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class ParticipantInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public ParticipantInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "ParticipantInitialInsert";
    public string Title => "Participant initial insert";
    public string Description => "Load court case participants from Production and insert into Staging court_case_participant.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/ParticipantInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .Cast<IDictionary<string, object>>()
            .ToList();

        var targetColumns = await ResolveTargetColumnsAsync(stagingConnection);

        var courtCaseByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `court_case`;", "id_api");
        var citizenByUuid = await LoadLookupAsync(stagingConnection, "SELECT id, uuid FROM `citizen`;", "uuid");
        var corporationByUuid = await LoadLookupAsync(stagingConnection, "SELECT id, uuid FROM `corporation`;", "uuid");

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `court_case_participant`;");

        var mappedRows = sourceRows
            .Select(row =>
            {
                var idApi = GetValue(row, "id_api");
                var courtCaseApiId = GetValue(row, "court_case_id_api")?.ToString();
                var memberType = GetValue(row, "member_type")?.ToString()?.Trim().ToLowerInvariant();
                var uuid = GetValue(row, "uuid")?.ToString()?.Trim();
                var participantIdentifier = GetValue(row, "participant_identifier");
                var joinedAt = GetValue(row, "joined_at");
                var leavedAt = GetValue(row, "leaved_at");

                var courtCaseId = courtCaseApiId is not null && courtCaseByApiId.TryGetValue(courtCaseApiId, out var mappedCourtCaseId)
                    ? mappedCourtCaseId
                    : null;

                object? memberId = null;
                if (!string.IsNullOrWhiteSpace(uuid))
                {
                    if (memberType == "citizen" && citizenByUuid.TryGetValue(uuid, out var citizenId))
                    {
                        memberId = citizenId;
                    }
                    else if (memberType == "corporation" && corporationByUuid.TryGetValue(uuid, out var corporationId))
                    {
                        memberId = corporationId;
                    }
                }

                return new
                {
                    idApi,
                    courtCaseId,
                    memberId,
                    participantIdentifier,
                    memberType,
                    joinedAt,
                    leavedAt,
                    uniqueKey = $"{idApi}|{courtCaseApiId}|{memberType}|{uuid}|{participantIdentifier}|{joinedAt}|{leavedAt}"
                };
            })
            .Where(x => x.courtCaseId is not null && x.memberId is not null)
            .GroupBy(x => x.uniqueKey, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();

        var insertRows = mappedRows
            .Select(row =>
            {
                var values = new Dictionary<string, object?>
                {
                    [targetColumns.CourtCaseIdColumn] = row.courtCaseId,
                    [targetColumns.MemberIdColumn] = row.memberId
                };

                if (!string.IsNullOrWhiteSpace(targetColumns.IdApiColumn))
                {
                    values[targetColumns.IdApiColumn!] = row.idApi;
                }

                if (!string.IsNullOrWhiteSpace(targetColumns.ParticipantTypeColumn))
                {
                    values[targetColumns.ParticipantTypeColumn!] = row.participantIdentifier;
                }

                if (!string.IsNullOrWhiteSpace(targetColumns.MemberTypeColumn))
                {
                    values[targetColumns.MemberTypeColumn!] = row.memberType;
                }

                if (!string.IsNullOrWhiteSpace(targetColumns.JoinedAtColumn))
                {
                    values[targetColumns.JoinedAtColumn!] = row.joinedAt;
                }

                if (!string.IsNullOrWhiteSpace(targetColumns.LeavedAtColumn))
                {
                    values[targetColumns.LeavedAtColumn!] = row.leavedAt;
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
                $"INSERT INTO `court_case_participant` ({columnList}) VALUES ({valuesList});",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string CourtCaseIdColumn, string MemberIdColumn, string? IdApiColumn, string? ParticipantTypeColumn, string? MemberTypeColumn, string? JoinedAtColumn, string? LeavedAtColumn)> ResolveTargetColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'court_case_participant';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var courtCaseIdColumn = new[] { "court_case_id", "courtcase_id", "case_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(courtCaseIdColumn))
        {
            throw new InvalidOperationException("Could not resolve court_case reference column in court_case_participant.");
        }

        var memberIdColumn = new[] { "member_id", "participant_id", "party_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(memberIdColumn))
        {
            throw new InvalidOperationException("Could not resolve member reference column in court_case_participant.");
        }

        var idApiColumn = new[] { "id_api" }.FirstOrDefault(columns.Contains);
        var participantTypeColumn = new[] { "participant_type_id", "participant_identifier", "participant_type" }.FirstOrDefault(columns.Contains);
        var memberTypeColumn = new[] { "member_type" }.FirstOrDefault(columns.Contains);
        var joinedAtColumn = new[] { "joined_at" }.FirstOrDefault(columns.Contains);
        var leavedAtColumn = new[] { "leaved_at" }.FirstOrDefault(columns.Contains);

        return (courtCaseIdColumn, memberIdColumn, idApiColumn, participantTypeColumn, memberTypeColumn, joinedAtColumn, leavedAtColumn);
    }

    private static async Task<Dictionary<string, object>> LoadLookupAsync(MySqlConnection stagingConnection, string query, string keyColumn)
    {
        var rows = (await stagingConnection.QueryAsync(
                query,
                commandTimeout: 120))
            .Cast<IDictionary<string, object>>()
            .ToList();

        return rows
            .Select(row => new
            {
                id = GetValue(row, "id"),
                key = GetValue(row, keyColumn)?.ToString()?.Trim()
            })
            .Where(x => x.id is not null && !string.IsNullOrWhiteSpace(x.key))
            .GroupBy(x => x.key!, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First().id!, StringComparer.OrdinalIgnoreCase);
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
