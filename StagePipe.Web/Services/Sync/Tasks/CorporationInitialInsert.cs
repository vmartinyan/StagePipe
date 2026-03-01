using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class CorporationInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public CorporationInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }
    
    public string Key => "CorporationInitialInsert";
    public string Title => "Corporation initial insert";
    public string Description => "Load initial corporation data from Production and insert into Staging corporation.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/CorporationInitialInsert.select.sql");

        var sourceRows = (await QuerySourceRowsWithTaxFallbackAsync(sourceConnection, selectQuery))
            .Cast<IDictionary<string, object>>()
            .ToList();

        var fallbackTaxByUuid = await GetFallbackTaxByUuidAsync(sourceConnection);

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `corporation`;");

        var insertRows = sourceRows
            .Select(row =>
            {
                var uuidValue = GetValue(row, "uuid")?.ToString();
                var taxValue = (GetValue(row, "tax_id") ?? GetValue(row, "taxId"))?.ToString();

                if (string.IsNullOrWhiteSpace(taxValue)
                    && !string.IsNullOrWhiteSpace(uuidValue)
                    && fallbackTaxByUuid.TryGetValue(uuidValue.Trim(), out var fallbackTaxValue)
                    && !string.IsNullOrWhiteSpace(fallbackTaxValue))
                {
                    taxValue = fallbackTaxValue;
                }

                return new
                {
                    uuid = uuidValue,
                    citizenship = GetValue(row, "citizenship")?.ToString(),
                    tax_id = taxValue,
                    name = GetValue(row, "name")?.ToString(),
                    type = GetValue(row, "type")?.ToString(),
                    slug = GetValue(row, "slug")?.ToString(),
                    unique_key = !string.IsNullOrWhiteSpace(uuidValue)
                        ? $"uuid:{uuidValue.Trim().ToLowerInvariant()}"
                        : (!string.IsNullOrWhiteSpace(taxValue)
                            ? $"tax_id:{taxValue.Trim()}"
                            : null)
                };
            })
            .Where(x => !string.IsNullOrWhiteSpace(x.unique_key))
            .GroupBy(x => x.unique_key!, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var selected = group
                    .OrderByDescending(x => !string.IsNullOrWhiteSpace(x.uuid))
                    .ThenByDescending(x => !string.IsNullOrWhiteSpace(x.tax_id))
                    .First();

                return new
                {
                    selected.uuid,
                    selected.citizenship,
                    selected.tax_id,
                    selected.name,
                    selected.type,
                    selected.slug
                };
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            await stagingConnection.ExecuteAsync(
                @"INSERT IGNORE INTO `corporation` (`uuid`, `citizenship`, `tax_id`, `name`, `type`, `slug`)
                  VALUES (@uuid, @citizenship, @tax_id, @name, @type, @slug);",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<Dictionary<string, string>> GetFallbackTaxByUuidAsync(MySqlConnection sourceConnection)
    {
        var queries = new[]
        {
            "SELECT c.uuid, c.tax_id AS tax_id FROM Corporations c WHERE c.tax_id IS NOT NULL",
            "SELECT c.uuid, c.taxId AS tax_id FROM Corporations c WHERE c.taxId IS NOT NULL",
            "SELECT c.uuid, p.tax_id AS tax_id FROM Corporations c JOIN Profiles p ON p.profileId = c.profileId WHERE p.tax_id IS NOT NULL",
            "SELECT c.uuid, p.taxId AS tax_id FROM Corporations c JOIN Profiles p ON p.profileId = c.profileId WHERE p.taxId IS NOT NULL"
        };

        var taxByUuid = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var query in queries)
        {
            try
            {
                var rows = (await sourceConnection.QueryAsync(query, commandTimeout: 120))
                    .Cast<IDictionary<string, object>>()
                    .ToList();

                foreach (var row in rows)
                {
                    var uuid = GetValue(row, "uuid")?.ToString()?.Trim();
                    var tax = GetValue(row, "tax_id")?.ToString()?.Trim();

                    if (string.IsNullOrWhiteSpace(uuid) || string.IsNullOrWhiteSpace(tax))
                    {
                        continue;
                    }

                    if (!taxByUuid.ContainsKey(uuid))
                    {
                        taxByUuid[uuid] = tax;
                    }
                }
            }
            catch (MySqlException ex) when (ex.Number == 1054 || ex.Number == 1146)
            {
            }
        }

        return taxByUuid;
    }

    private static async Task<IEnumerable<dynamic>> QuerySourceRowsWithTaxFallbackAsync(MySqlConnection sourceConnection, string selectQuery)
    {
        try
        {
            return await sourceConnection.QueryAsync(selectQuery, commandTimeout: 180);
        }
        catch (MySqlException ex) when (ex.Number == 1054 && ex.Message.Contains("taxId", StringComparison.OrdinalIgnoreCase))
        {
            var fallbackQuery = selectQuery
                .Replace("c.taxId", "c.tax_id", StringComparison.OrdinalIgnoreCase)
                .Replace("`taxId`", "`tax_id`", StringComparison.OrdinalIgnoreCase)
                .Replace(" taxId ", " tax_id ", StringComparison.OrdinalIgnoreCase);

            return await sourceConnection.QueryAsync(fallbackQuery, commandTimeout: 180);
        }
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
