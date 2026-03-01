using Dapper;
using Microsoft.Extensions.Configuration;
using MySqlConnector;
using System.Globalization;
using StagePipe.Web.Services.Connectivity;

namespace StagePipe.Web.Services.Sync;

public sealed class JudgeInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;
    private readonly IConfiguration _configuration;
    private readonly ISshTunnelManager _sshTunnelManager;

    public JudgeInitialInsert(
        ISqlScriptProvider sqlScriptProvider,
        IConfiguration configuration,
        ISshTunnelManager sshTunnelManager)
    {
        _sqlScriptProvider = sqlScriptProvider;
        _configuration = configuration;
        _sshTunnelManager = sshTunnelManager;
    }

    public string Key => "JudgeInitialInsert";
    public string Title => "Judge initial insert";
    public string Description => "Load judges from auth users/profiles filtered by system role 1 into staging judge table.";
    public string SourceDatabase => "ProductionAuth";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/JudgeInitialInsert.select.sql");
        var apiUsersQuery = _sqlScriptProvider.GetScript("Sync/ProductionApiUsers.select.sql");
        var productionConnectionString = _configuration.GetConnectionString("Production");

        if (string.IsNullOrWhiteSpace(productionConnectionString))
        {
            throw new InvalidOperationException("Missing Production connection string.");
        }

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .Cast<IDictionary<string, object>>()
            .ToList();

        await _sshTunnelManager.EnsureForDatabaseAsync("Production", cancellationToken);

        await using var productionConnection = new MySqlConnection(productionConnectionString);
        await productionConnection.OpenAsync(cancellationToken);

        var apiUserRows = (await productionConnection.QueryAsync(
                apiUsersQuery,
                commandTimeout: 180))
            .Cast<IDictionary<string, object>>()
            .ToList();

        var apiUserIdByUuid = apiUserRows
            .Select(row => new
            {
                uuid = row.TryGetValue("uuid", out var uuidValue) && uuidValue is not DBNull ? uuidValue?.ToString() : null,
                id_api = row.TryGetValue("id_api", out var idApiValue) && idApiValue is not DBNull ? idApiValue : null
            })
            .Where(x => !string.IsNullOrWhiteSpace(x.uuid))
            .GroupBy(x => x.uuid!, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First().id_api, StringComparer.OrdinalIgnoreCase);

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `judge_history`;");
        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `judge`;");

        var sourceMappedRows = sourceRows
            .Select(row => new
            {
                id_auth = row.TryGetValue("id_auth", out var idValue) && idValue is not DBNull ? idValue : null,
                uuid = row.TryGetValue("uuid", out var uuidValue) && uuidValue is not DBNull ? uuidValue?.ToString() : null,
                id_api = row.TryGetValue("uuid", out var lookupUuidValue)
                    && lookupUuidValue is not DBNull
                    && lookupUuidValue?.ToString() is { } lookupUuid
                    && apiUserIdByUuid.TryGetValue(lookupUuid, out var matchedApiId)
                    ? matchedApiId
                    : null,
                instance_id = row.TryGetValue("instance_id", out var instanceIdValue)
                    && instanceIdValue is not DBNull
                    && int.TryParse(Convert.ToString(instanceIdValue, CultureInfo.InvariantCulture), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInstanceId)
                    ? parsedInstanceId
                    : (int?)null,
                psn = row.TryGetValue("psn", out var psnValue) && psnValue is not DBNull ? psnValue?.ToString() : null,
                first_name = row.TryGetValue("first_name", out var firstNameValue) && firstNameValue is not DBNull ? firstNameValue?.ToString() : null,
                last_name = row.TryGetValue("last_name", out var lastNameValue) && lastNameValue is not DBNull ? lastNameValue?.ToString() : null,
                patronymic = row.TryGetValue("patronymic", out var patronymicValue) && patronymicValue is not DBNull ? patronymicValue?.ToString() : null,
                birth_date = row.TryGetValue("birth_date", out var birthDateValue) && birthDateValue is not DBNull ? birthDateValue : null,
                death_date = row.TryGetValue("death_date", out var deathDateValue) && deathDateValue is not DBNull ? deathDateValue : null,
                gender = row.TryGetValue("gender", out var genderValue) && genderValue is not DBNull ? genderValue?.ToString() : null
            })
            .ToList();

        var judgeRows = sourceMappedRows
            .Select(row => new
            {
                row.id_auth,
                row.uuid,
                row.id_api,
                row.psn,
                row.first_name,
                row.last_name,
                row.patronymic,
                row.birth_date,
                row.death_date,
                row.gender,
                unique_key = row.id_auth is not null
                    ? $"auth:{Convert.ToString(row.id_auth, CultureInfo.InvariantCulture)}"
                    : (!string.IsNullOrWhiteSpace(row.uuid) ? $"uuid:{row.uuid.Trim().ToLowerInvariant()}" : null)
            })
            .Where(x => !string.IsNullOrWhiteSpace(x.unique_key))
            .GroupBy(x => x.unique_key!, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .ToList();

        if (judgeRows.Count > 0)
        {
            await stagingConnection.ExecuteAsync(
                                @"INSERT IGNORE INTO `judge` (`id_auth`, `uuid`, `id_api`, `psn`, `first_name`, `last_name`, `patronymic`, `birth_date`, `death_date`, `gender`)
                                    VALUES (@id_auth, @uuid, @id_api, @psn, @first_name, @last_name, @patronymic, @birth_date, @death_date, @gender);",
            judgeRows,
                commandTimeout: 180);

            var judges = (await stagingConnection.QueryAsync(
                    "SELECT id, id_auth FROM `judge`;",
                    commandTimeout: 180))
                .Cast<IDictionary<string, object>>()
                .ToList();

            var judgeIdByAuthId = judges
                .Select(row => new
                {
                    id = row.TryGetValue("id", out var judgeIdValue) && judgeIdValue is not DBNull ? judgeIdValue : null,
                    id_auth_key = row.TryGetValue("id_auth", out var authIdValue) && authIdValue is not DBNull
                        ? Convert.ToString(authIdValue, CultureInfo.InvariantCulture)
                        : null
                })
                .Where(x => x.id is not null && !string.IsNullOrWhiteSpace(x.id_auth_key))
                .GroupBy(x => x.id_auth_key!, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(group => group.Key, group => group.First().id, StringComparer.OrdinalIgnoreCase);

            var joinedAt = new DateTime(2025, 8, 25);

            var historyRows = sourceMappedRows
                .Select(row => new
                {
                    judge_id = row.id_auth is not null
                        && Convert.ToString(row.id_auth, CultureInfo.InvariantCulture) is { } idAuthKey
                        && judgeIdByAuthId.TryGetValue(idAuthKey, out var mappedJudgeId)
                        ? mappedJudgeId
                        : null,
                    instance_id = row.instance_id,
                    joined_at = joinedAt,
                    leaved_at = (DateTime?)null
                })
                .Where(x => x.judge_id is not null)
                .GroupBy(x => new { x.judge_id, x.instance_id })
                .Select(group => group.First())
                .ToList();

            if (historyRows.Count > 0)
            {
                await stagingConnection.ExecuteAsync(
                    @"INSERT INTO `judge_history` (`judge_id`, `instance_id`, `joined_at`, `leaved_at`)
                      VALUES (@judge_id, @instance_id, @joined_at, @leaved_at);",
                    historyRows,
                    commandTimeout: 180);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        return judgeRows.Count;
    }
}
