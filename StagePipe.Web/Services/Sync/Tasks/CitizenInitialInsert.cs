using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class CitizenInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public CitizenInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "CitizenInitialInsert";
    public string Title => "Citizen initial insert";
    public string Description => "Load initial citizen data from Production and insert into Staging citizen.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/CitizenInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .Cast<IDictionary<string, object>>()
            .ToList();

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `citizen`;");

        var insertRows = sourceRows
            .Select(row => new
            {
                uuid = GetValue(row, "uuid")?.ToString(),
                citizenship = GetValue(row, "citizenship")?.ToString(),
                psn = GetValue(row, "psn")?.ToString(),
                first_name = GetValue(row, "first_name")?.ToString(),
                last_name = GetValue(row, "last_name")?.ToString(),
                patronymic = GetValue(row, "patronymic")?.ToString(),
                birth_date = GetValue(row, "birth_date"),
                death_date = GetValue(row, "death_date"),
                gender = GetValue(row, "gender")?.ToString(),
                unique_key = !string.IsNullOrWhiteSpace(GetValue(row, "uuid")?.ToString())
                    ? $"uuid:{GetValue(row, "uuid")!.ToString()!.Trim().ToLowerInvariant()}"
                    : (!string.IsNullOrWhiteSpace(GetValue(row, "psn")?.ToString())
                        ? $"psn:{GetValue(row, "psn")!.ToString()!.Trim()}"
                        : null)
            })
            .Where(x => !string.IsNullOrWhiteSpace(x.unique_key))
            .GroupBy(x => x.unique_key!, StringComparer.OrdinalIgnoreCase)
            .Select(group =>
            {
                var selected = group
                    .OrderByDescending(x => !string.IsNullOrWhiteSpace(x.uuid))
                    .ThenByDescending(x => !string.IsNullOrWhiteSpace(x.psn))
                    .First();

                return new
                {
                    selected.uuid,
                    selected.citizenship,
                    selected.psn,
                    selected.first_name,
                    selected.last_name,
                    selected.patronymic,
                    selected.birth_date,
                    selected.death_date,
                    selected.gender
                };
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            await stagingConnection.ExecuteAsync(
                @"INSERT IGNORE INTO `citizen` (`uuid`, `citizenship`, `psn`, `first_name`, `last_name`, `patronymic`, `birth_date`, `death_date`, `gender`)
                  VALUES (@uuid, @citizenship, @psn, @first_name, @last_name, @patronymic, @birth_date, @death_date, @gender);",
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
