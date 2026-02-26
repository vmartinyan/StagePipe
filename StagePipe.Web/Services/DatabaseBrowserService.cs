using Dapper;
using MySqlConnector;
using StagePipe.Web.Models;
using StagePipe.Web.Services.Connectivity;

namespace StagePipe.Web.Services;

public sealed class DatabaseBrowserService : IDatabaseBrowserService
{
    private readonly IConfiguration _configuration;
    private readonly ISshTunnelManager _sshTunnelManager;

    public DatabaseBrowserService(IConfiguration configuration, ISshTunnelManager sshTunnelManager)
    {
        _configuration = configuration;
        _sshTunnelManager = sshTunnelManager;
    }

    public async Task<BrowsePageState> LoadAsync(string selectedDb, string selectedTable, int limit, CancellationToken cancellationToken)
    {
        var state = new BrowsePageState
        {
            SelectedDb = NormalizeDb(selectedDb),
            SelectedTable = selectedTable,
            Limit = ParseLimit(limit)
        };

        var connectionString = _configuration.GetConnectionString(state.SelectedDb);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            state.BrowseError = $"Connection string for '{state.SelectedDb}' is missing.";
            return state;
        }

        await _sshTunnelManager.EnsureForDatabaseAsync(state.SelectedDb, cancellationToken);

        await using var connection = new MySqlConnection(connectionString);

        try
        {
            await connection.OpenAsync(cancellationToken);

            state.Tables = (await connection.QueryAsync<string>(
                    "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = DATABASE() ORDER BY TABLE_NAME;"))
                .ToList();

            if (string.IsNullOrWhiteSpace(state.SelectedTable) && state.Tables.Count > 0)
            {
                state.SelectedTable = state.Tables[0];
            }

            if (string.IsNullOrWhiteSpace(state.SelectedTable))
            {
                return state;
            }

            if (!state.Tables.Contains(state.SelectedTable, StringComparer.OrdinalIgnoreCase))
            {
                state.BrowseError = $"Table '{state.SelectedTable}' was not found in the selected database.";
                return state;
            }

            var sql = $"SELECT * FROM {QuoteIdentifier(state.SelectedTable)} LIMIT @limit;";
            var result = (await connection.QueryAsync(
                    sql,
                    new { limit = state.Limit },
                    commandTimeout: 120))
                .Cast<IDictionary<string, object>>()
                .ToList();

            state.Rows = result
                .Select(row => row.ToDictionary(
                    pair => pair.Key,
                    pair => pair.Value is DBNull ? null : pair.Value,
                    StringComparer.OrdinalIgnoreCase))
                .Cast<IDictionary<string, object?>>()
                .ToList();
        }
        catch (Exception ex)
        {
            state.BrowseError = BuildConnectionError(state.SelectedDb, connectionString, ex);
        }

        return state;
    }

    private static string NormalizeDb(string? db)
    {
        if (string.Equals(db, "Production", StringComparison.OrdinalIgnoreCase))
        {
            return "Production";
        }

        if (string.Equals(db, "ProductionAuth", StringComparison.OrdinalIgnoreCase))
        {
            return "ProductionAuth";
        }

        return "Staging";
    }

    private static int ParseLimit(int limit)
    {
        if (limit <= 0)
        {
            return 100;
        }

        return Math.Clamp(limit, 1, 500);
    }

    private static string QuoteIdentifier(string identifier)
    {
        var escaped = identifier.Replace("`", "``", StringComparison.Ordinal);
        return $"`{escaped}`";
    }

    private static string BuildConnectionError(string dbName, string connectionString, Exception ex)
    {
        var details = new List<string>();

        try
        {
            var builder = new MySqlConnectionStringBuilder(connectionString);
            details.Add($"Database={dbName}");
            details.Add($"Host={builder.Server}");
            details.Add($"Port={builder.Port}");
            details.Add($"DbName={builder.Database}");
        }
        catch
        {
            details.Add($"Database={dbName}");
        }

        var root = ex;
        while (root.InnerException is not null)
        {
            root = root.InnerException;
        }

        return $"{string.Join(", ", details)}. Error: {root.Message}";
    }
}
