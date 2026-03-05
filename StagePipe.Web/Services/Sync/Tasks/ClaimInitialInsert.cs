using Dapper;
using MySqlConnector;
using System.Net;
using System.Text.RegularExpressions;

namespace StagePipe.Web.Services.Sync;

public sealed class ClaimInitialInsert : ISyncTask
{
    private static readonly Regex HtmlTagRegex = new("<[^>]+>", RegexOptions.Compiled);

    private readonly ISqlScriptProvider _sqlScriptProvider;

    public ClaimInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "ClaimInitialInsert";
    public string Title => "Claim initial insert";
    public string Description => "Load claim title/description from Production API and insert plain-text description into Staging claim.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/ClaimInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .Cast<IDictionary<string, object>>()
            .ToList();

        var courtCases = (await stagingConnection.QueryAsync(
                "SELECT id, id_api FROM `court_case`;",
                commandTimeout: 180))
            .Cast<IDictionary<string, object>>()
            .ToList();

        var courtCaseIdByApiId = courtCases
            .Select(row => new
            {
                id = GetValue(row, "id"),
                id_api_key = GetValue(row, "id_api")?.ToString()
            })
            .Where(x => x.id is not null && !string.IsNullOrWhiteSpace(x.id_api_key))
            .GroupBy(x => x.id_api_key!, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First().id, StringComparer.OrdinalIgnoreCase);

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `claim`;");

        var insertRows = sourceRows
            .Select(row =>
            {
                var courtCaseApiId = GetValue(row, "court_case_id_api")?.ToString();
                var hasMappedCourtCase = courtCaseApiId is not null && courtCaseIdByApiId.ContainsKey(courtCaseApiId);
                var courtCaseId = hasMappedCourtCase ? courtCaseIdByApiId[courtCaseApiId!] : null;

                return new
                {
                    court_case_id = courtCaseId,
                    has_mapped_court_case = hasMappedCourtCase,
                    title = GetValue(row, "title")?.ToString(),
                    description = StripHtmlToText(GetValue(row, "description")?.ToString()),
                    unique_key = courtCaseId
                };
            })
            .Where(x => x.has_mapped_court_case && x.court_case_id is not null)
            .GroupBy(x => x.unique_key)
            .Select(group =>
            {
                var selected = group
                    .OrderByDescending(x => !string.IsNullOrWhiteSpace(x.description))
                    .ThenByDescending(x => !string.IsNullOrWhiteSpace(x.title))
                    .First();

                return new
                {
                    selected.court_case_id,
                    selected.title,
                    selected.description
                };
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            await stagingConnection.ExecuteAsync(
                                @"INSERT INTO `claim` (`court_case_id`, `title`, `description`)
                                    VALUES (@court_case_id, @title, @description);",
                insertRows,
                commandTimeout: 180);
        }

        await stagingConnection.ExecuteAsync(
                @"UPDATE `court_case` cc
                                                        JOIN `claim` c ON c.`court_case_id` = cc.`id`
                            SET cc.`claim_id` = c.`id`;",
                commandTimeout: 180);

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

    private static string? StripHtmlToText(string? html)
    {
        if (string.IsNullOrWhiteSpace(html))
        {
            return html;
        }

        var noTags = HtmlTagRegex.Replace(html, " ");
        var decoded = WebUtility.HtmlDecode(noTags);
        var normalizedWhitespace = Regex.Replace(decoded, "\\s+", " ").Trim();

        return normalizedWhitespace;
    }
}
