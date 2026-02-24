namespace DatabaseEngineWindowsService;

using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Reflection;

internal sealed record SqlExecuteRequest(string Sql, string? Database = null);

internal sealed record SqlExecuteResponse(
    bool IsQuery,
    int AffectedRows,
    IReadOnlyList<IDictionary<string, object?>>? Rows);

internal sealed record DatabaseSummaryResponse(string Name, int TableCount);

internal static class SqlResponseMapper
{
    public static SqlExecuteResponse FromResult(SqlExecutionResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new SqlExecuteResponse(result.IsQuery, result.AffectedRows, ConvertRows(result.Rows));
    }

    private static IReadOnlyList<IDictionary<string, object?>>? ConvertRows(IReadOnlyList<object>? rows)
    {
        if (rows is null || rows.Count == 0)
            return null;

        var list = new List<IDictionary<string, object?>>(rows.Count);
        foreach (var row in rows)
        {
            if (row is null)
            {
                list.Add(new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));
                continue;
            }

            if (row is IDictionary<string, object?> dictionary)
            {
                list.Add(new Dictionary<string, object?>(dictionary, StringComparer.OrdinalIgnoreCase));
                continue;
            }

            var snapshot = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var property in row.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                snapshot[property.Name] = property.GetValue(row);
            }

            list.Add(snapshot);
        }

        return list;
    }
}

internal static class DatabaseResponseMapper
{
    public static DatabaseSummaryResponse From(Database database)
    {
        ArgumentNullException.ThrowIfNull(database);
        return new DatabaseSummaryResponse(database.Name, database.Tables?.Count ?? 0);
    }
}
