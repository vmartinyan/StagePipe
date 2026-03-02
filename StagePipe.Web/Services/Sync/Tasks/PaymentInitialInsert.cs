using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class PaymentInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public PaymentInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "PaymentInitialInsert";
    public string Title => "Payment initial insert";
    public string Description => "Load payments from Production API and insert into Staging payment using mapped court_case_id.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/PaymentInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var paymentColumns = await ResolvePaymentColumnsAsync(stagingConnection);
        var courtCaseLookup = await LoadCourtCaseLookupAsync(stagingConnection);

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `payment`;");

        var mappedRows = sourceRows
            .Select(row =>
            {
                var courtCaseApiId = GetValue(row, "court_case_id_api")?.ToString()?.Trim();
                var mappedCourtCaseId = courtCaseApiId is not null && courtCaseLookup.TryGetValue(courtCaseApiId, out var id)
                    ? id
                    : null;

                return new
                {
                    courtCaseApiId,
                    courtCaseId = mappedCourtCaseId,
                    amount = GetValue(row, "amount"),
                    paymentDate = GetValue(row, "payment_date"),
                    orderNumber = GetValue(row, "order_number")?.ToString(),
                    type = GetValue(row, "type")?.ToString()
                };
            })
            .Where(x => x.courtCaseId is not null)
            .ToList();

        var insertRows = mappedRows
            .Select(row =>
            {
                var values = new Dictionary<string, object?>
                {
                    [paymentColumns.CourtCaseIdColumn] = row.courtCaseId,
                    [paymentColumns.AmountColumn] = row.amount,
                    [paymentColumns.PaymentDateColumn] = row.paymentDate,
                    [paymentColumns.OrderNumberColumn] = row.orderNumber,
                    [paymentColumns.TypeColumn] = row.type
                };

                if (paymentColumns.CourtCaseIdApiColumn is not null)
                {
                    values[paymentColumns.CourtCaseIdApiColumn] = row.courtCaseApiId;
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
                $"INSERT INTO `payment` ({columnList}) VALUES ({valuesList});",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string CourtCaseIdColumn, string AmountColumn, string PaymentDateColumn, string OrderNumberColumn, string TypeColumn, string? CourtCaseIdApiColumn)> ResolvePaymentColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'payment';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var courtCaseIdColumn = new[] { "court_case_id", "courtcase_id", "case_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(courtCaseIdColumn))
        {
            throw new InvalidOperationException("Could not resolve court_case reference column in payment.");
        }

        var amountColumn = new[] { "amount", "sum", "total", "payment_amount" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(amountColumn))
        {
            throw new InvalidOperationException("Could not resolve amount column in payment.");
        }

        var paymentDateColumn = new[] { "payment_date", "paid_at", "date", "created_at", "updated_at" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(paymentDateColumn))
        {
            throw new InvalidOperationException("Could not resolve payment date column in payment.");
        }

        var orderNumberColumn = new[] { "order_number", "order_no", "pin", "number" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(orderNumberColumn))
        {
            throw new InvalidOperationException("Could not resolve order number column in payment.");
        }

        var typeColumn = new[] { "type", "payment_type", "source_type" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(typeColumn))
        {
            throw new InvalidOperationException("Could not resolve type column in payment.");
        }

        var courtCaseIdApiColumn = new[] { "court_case_id_api", "case_id_api", "courtcase_id_api" }.FirstOrDefault(columns.Contains);

        return (courtCaseIdColumn, amountColumn, paymentDateColumn, orderNumberColumn, typeColumn, courtCaseIdApiColumn);
    }

    private static async Task<Dictionary<string, object>> LoadCourtCaseLookupAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'court_case';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var apiKeyColumn = new[] { "ai_api", "id_api", "api_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(apiKeyColumn))
        {
            throw new InvalidOperationException("Could not resolve API key column in court_case. Expected one of: ai_api, id_api, api_id.");
        }

        var rows = (await stagingConnection.QueryAsync(
                $"SELECT id, `{apiKeyColumn}` AS api_key FROM `court_case`;",
                commandTimeout: 120))
            .OfType<IDictionary<string, object>>()
            .ToList();

        return rows
            .Select(row => new
            {
                id = GetValue(row, "id"),
                key = GetValue(row, "api_key")?.ToString()?.Trim()
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