using Dapper;
using MySqlConnector;

namespace StagePipe.Web.Services.Sync;

public sealed class StateDutyInitialInsert : ISyncTask
{
    private readonly ISqlScriptProvider _sqlScriptProvider;

    public StateDutyInitialInsert(ISqlScriptProvider sqlScriptProvider)
    {
        _sqlScriptProvider = sqlScriptProvider;
    }

    public string Key => "StateDutyInitialInsert";
    public string Title => "State duty initial insert";
    public string Description => "Load state duty rows from Production API and insert into Staging state_duty with mapped court_case_id.";

    public async Task<int> ExecuteAsync(MySqlConnection sourceConnection, MySqlConnection stagingConnection, CancellationToken cancellationToken)
    {
        var selectQuery = _sqlScriptProvider.GetScript("Sync/StateDutyInitialInsert.select.sql");

        var sourceRows = (await sourceConnection.QueryAsync(
                selectQuery,
                commandTimeout: 180))
            .OfType<IDictionary<string, object>>()
            .ToList();

        var targetColumns = await ResolveTargetColumnsAsync(stagingConnection);
        var courtCaseByApiId = await LoadCourtCaseLookupByIdApiAsync(stagingConnection);

        await stagingConnection.ExecuteAsync("TRUNCATE TABLE `state_duty`;");

        var mappedRows = sourceRows
            .Select(row =>
            {
                var courtCaseApiId = GetValue(row, "court_case_id_api")?.ToString()?.Trim();
                object? mappedCourtCaseId = null;
                var hasMappedCourtCase = courtCaseApiId is not null && courtCaseByApiId.TryGetValue(courtCaseApiId, out mappedCourtCaseId);

                return new
                {
                    hasMappedCourtCase,
                    courtCaseId = hasMappedCourtCase ? mappedCourtCaseId : null,
                    pin = GetValue(row, "pin")?.ToString(),
                    date = GetValue(row, "date"),
                    paidType = GetValue(row, "paid_type")?.ToString(),
                    checkNumber = GetValue(row, "check_number")?.ToString(),
                    receiptNumber = GetValue(row, "receipt_number")?.ToString()
                };
            })
            .Where(x => x.hasMappedCourtCase)
            .ToList();

        var insertRows = mappedRows
            .Select(row =>
            {
                var values = new Dictionary<string, object?>
                {
                    [targetColumns.CourtCaseIdColumn] = row.courtCaseId,
                    [targetColumns.PinColumn] = row.pin,
                    [targetColumns.DateColumn] = row.date,
                    [targetColumns.PaidTypeColumn] = row.paidType,
                    [targetColumns.CheckNumberColumn] = row.checkNumber,
                    [targetColumns.ReceiptNumberColumn] = row.receiptNumber
                };

                return values;
            })
            .ToList();

        if (insertRows.Count > 0)
        {
            var columns = insertRows[0].Keys.ToList();
            var columnList = string.Join(", ", columns.Select(column => $"`{column}`"));
            var valuesList = string.Join(", ", columns.Select(column => $"@{column}"));

            await stagingConnection.ExecuteAsync(
                $"INSERT INTO `state_duty` ({columnList}) VALUES ({valuesList});",
                insertRows,
                commandTimeout: 180);
        }

        cancellationToken.ThrowIfCancellationRequested();
        return insertRows.Count;
    }

    private static async Task<(string CourtCaseIdColumn, string PinColumn, string DateColumn, string PaidTypeColumn, string CheckNumberColumn, string ReceiptNumberColumn)> ResolveTargetColumnsAsync(MySqlConnection stagingConnection)
    {
        var columns = (await stagingConnection.QueryAsync<string>(
                @"SELECT COLUMN_NAME
                  FROM INFORMATION_SCHEMA.COLUMNS
                  WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = 'state_duty';",
                commandTimeout: 60))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var courtCaseIdColumn = new[] { "court_case_id", "courtcase_id", "case_id" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(courtCaseIdColumn))
        {
            throw new InvalidOperationException("Could not resolve court_case reference column in state_duty.");
        }

        var pinColumn = new[] { "pin" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(pinColumn))
        {
            throw new InvalidOperationException("Could not resolve pin column in state_duty.");
        }

        var dateColumn = new[] { "date", "paid_at", "payment_date", "created_at" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(dateColumn))
        {
            throw new InvalidOperationException("Could not resolve date column in state_duty.");
        }

        var paidTypeColumn = new[] { "paid_type", "type", "payment_type" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(paidTypeColumn))
        {
            throw new InvalidOperationException("Could not resolve paid_type column in state_duty.");
        }

        var checkNumberColumn = new[] { "check_number", "check_no", "check" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(checkNumberColumn))
        {
            throw new InvalidOperationException("Could not resolve check_number column in state_duty.");
        }

        var receiptNumberColumn = new[] { "receipt_number", "receipt_no", "receipt" }.FirstOrDefault(columns.Contains);
        if (string.IsNullOrWhiteSpace(receiptNumberColumn))
        {
            throw new InvalidOperationException("Could not resolve receipt_number column in state_duty.");
        }

        return (courtCaseIdColumn, pinColumn, dateColumn, paidTypeColumn, checkNumberColumn, receiptNumberColumn);
    }

    private static async Task<Dictionary<string, object>> LoadCourtCaseLookupByIdApiAsync(MySqlConnection stagingConnection)
    {
        var rows = (await stagingConnection.QueryAsync(
                "SELECT id, id_api FROM `court_case`;",
                commandTimeout: 120))
            .OfType<IDictionary<string, object>>()
            .ToList();

        return rows
            .Select(row => new
            {
                id = GetValue(row, "id"),
                key = GetValue(row, "id_api")?.ToString()?.Trim()
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