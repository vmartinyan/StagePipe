using Dapper;
using MySqlConnector;
using System.Net;
using System.Text.RegularExpressions;

namespace StagePipe.Web.Services.Sync;

public sealed class ClaimRequirementsInitialInsert : ISyncTask
{
    private static readonly Regex BreakRegex = new("<br\\s*/?>|</p>|</div>", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex TagRegex = new("<[^>]+>", RegexOptions.Compiled);
    private static readonly Regex UnclosedTagRegex = new("</?[a-zA-Z][^>\\r\\n]*", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static readonly Regex WhitespaceRegex = new("\\s+", RegexOptions.Compiled);

    private readonly ISqlScriptProvider _sqlScriptProvider;

    public ClaimRequirementsInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "ClaimRequirementsInitialInsert";
    public string Title => "Claim requirements initial insert";
    public string Description => "Load claim requirements from Production and insert into Staging claim_requirement with mapped IDs and plain-text description.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/ClaimRequirementsInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var targetColumns = await ResolveTargetColumnsAsync(stagingConnection);

        var courtCaseByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `court_case`;", "id_api");
        var participantByApiId = await LoadLookupAsync(stagingConnection, "SELECT id, id_api FROM `court_case_participant`;", "id_api");

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `claim_requirement`;");

        var mappedRows = sourceRows
            .Select(row =>
            {
                var courtCaseApiId = GetValue(row, "court_case_id_api")?.ToString()?.Trim();
                var participantApiId = GetValue(row, "participant_id_api")?.ToString()?.Trim();

                var courtCaseId = courtCaseApiId is not null && courtCaseByApiId.TryGetValue(courtCaseApiId, out var mappedCourtCaseId)
                    ? mappedCourtCaseId
                    : null;

                var participantId = participantApiId is not null && participantByApiId.TryGetValue(participantApiId, out var mappedParticipantId)
                    ? mappedParticipantId
                    : null;

                var description = FormatHtmlToPlainText(GetValue(row, "description")?.ToString());

                return new
                {
                    courtCaseId,
                    participantId,
                    description,
                    uniqueKey = $"{courtCaseApiId ?? string.Empty}|{participantApiId ?? string.Empty}|{description ?? string.Empty}"
                };
            })
            .Where(x => x.courtCaseId is not null && x.participantId is not null)
            .GroupBy(x => x.uniqueKey, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();

        var insertRows = mappedRows
            .Select(row => new Dictionary<string, object?>
            {
                [targetColumns.CourtCaseIdColumn] = row.courtCaseId,
                [targetColumns.ParticipantIdColumn] = row.participantId,
                [targetColumns.DescriptionColumn] = row.description
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            var columns = insertRows[0].Keys.ToList();
            var columnList = string.Join(", ", columns.Select(column => $"`{column}`"));
            var valuesList = string.Join(", ", columns.Select(column => $"@{column}"));

            await stagingConnection.ExecuteAsync(
                $"INSERT INTO `claim_requirement` ({columnList}) VALUES ({valuesList});",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string CourtCaseIdColumn, string ParticipantIdColumn, string DescriptionColumn)> ResolveTargetColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'claim_requirement';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var courtCaseIdColumn = new[] { "court_case_id", "courtcase_id", "case_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(courtCaseIdColumn))
        {
            throw new InvalidOperationException("Could not resolve court_case reference column in claim_requirement.");
        }

        var participantIdColumn = new[] { "participant_id", "court_case_participant_id", "party_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(participantIdColumn))
        {
            throw new InvalidOperationException("Could not resolve participant reference column in claim_requirement.");
        }

        var descriptionColumn = new[] { "description", "text", "content", "body" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(descriptionColumn))
        {
            throw new InvalidOperationException("Could not resolve description column in claim_requirement.");
        }

        return (courtCaseIdColumn, participantIdColumn, descriptionColumn);
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

    private static string? FormatHtmlToPlainText(string? html)
    {
        if (string.IsNullOrWhiteSpace(html))
        {
            return html;
        }

        var text = html;
        text = BreakRegex.Replace(text, " ");
        text = TagRegex.Replace(text, " ");
        text = WebUtility.HtmlDecode(text) ?? string.Empty;
        text = UnclosedTagRegex.Replace(text, " ");
        text = WhitespaceRegex.Replace(text, " ").Trim();

        return text;
    }
}
