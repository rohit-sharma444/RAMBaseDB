namespace RAMBaseDB.Application;

using RAMBaseDB.Domain.Abstractions;
using RAMBaseDB.Domain.Entities;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;


/// <summary>
/// Provides methods for parsing and executing simplified SQL statements against in-memory database tables.
/// </summary>
/// <remarks>SqlParser supports SELECT, INSERT, UPDATE, and DELETE commands using a subset of standard SQL syntax.
/// It routes SQL text to the appropriate command handler and materializes results using dynamic LINQ queries. The
/// parser requires a target database to be specified either via constructor or per execution. Only explicit column
/// lists are supported for INSERT statements, and JOINs are limited to INNER and LEFT types. This class is not
/// thread-safe and is intended for use with the provided DatabaseManager abstraction.</remarks>
public sealed class SqlParser
{
    private const string DefaultDatabaseName = "default";
    private readonly DatabaseEngine _databaseManager;
    private readonly string? _defaultDatabaseName;
    private readonly SqlExecutionResult sqlExecutionResultObject = new();

    private readonly ParsingConfig DynamicParsingConfig = new()
    {
        EvaluateGroupByAtDatabase = false
    };

    private readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, Func<object, object?>>> PropertyAccessorCache = new();

    public SqlParser()
        : this(new DatabaseEngine(new Database { Name = DefaultDatabaseName }), DefaultDatabaseName)
    {
    }

    public SqlParser(DatabaseEngine databaseManager, string? defaultDatabaseName = null)
    {
        _databaseManager = databaseManager ?? throw new ArgumentNullException(nameof(databaseManager));
        _defaultDatabaseName = string.IsNullOrWhiteSpace(defaultDatabaseName)
            ? null
            : defaultDatabaseName.Trim();
    }

    /// <summary>
    /// Entry point that routes the SQL text to the appropriate command handler.
    /// </summary>
    public SqlExecutionResult Execute(string sql)
        => Execute(sql, null);

    /// <summary>
    /// Executes the provided SQL statement against the specified database when supplied.
    /// </summary>
    public SqlExecutionResult Execute(string sql, string? databaseName)
    {
        if (string.IsNullOrWhiteSpace(sql))
            throw new ArgumentException("SQL statement cannot be empty.", nameof(sql));

        sql = sql.Trim().TrimEnd(';');
        var command = sql.Split(' ', StringSplitOptions.RemoveEmptyEntries)[0].ToUpperInvariant();
        var targetDatabase = ResolveDatabaseName(databaseName);

        return command switch
        {
            "SELECT" => ExecuteSelect(targetDatabase, ParseSelect(sql)),
            "INSERT" => ExecuteInsert(targetDatabase, ParseInsert(sql)),
            "UPDATE" => ExecuteUpdate(targetDatabase, ParseUpdate(sql)),
            "DELETE" => ExecuteDelete(targetDatabase, ParseDelete(sql)),
            _ => throw new NotSupportedException($"Command '{command}' is not supported.")
        };
    }

    #region "private functions"
    /// <summary>
    /// Builds a dynamic LINQ query to evaluate a SELECT command over the joined table data.
    /// </summary>
    private SqlExecutionResult ExecuteSelect(string databaseName, SelectCommand command)
    {
        // Assemble row contexts from the FROM table and any JOIN clauses.
        var contexts = BuildJoinContexts(databaseName, command);
        IQueryable<IDictionary<string, object?>> query = contexts.AsQueryable();

        if (!string.IsNullOrWhiteSpace(command.WhereClause))
        {
            var whereExpression = NormalizeBooleanExpression(command.WhereClause);
            query = query.Where(DynamicParsingConfig, whereExpression);
        }

        IQueryable selectQuery;

        if (command.GroupByColumns.Count > 0)
        {
            // When GROUP BY is present we build composite keys and aggregate within each bucket.
            var keyProjection = BuildGroupKeyProjection(command.GroupByColumns);
            var grouped = query.GroupBy(DynamicParsingConfig, keyProjection, "it");

            var projection = BuildGroupedSelectProjection(command.SelectColumns, command.GroupByColumns);
            selectQuery = grouped.Select(DynamicParsingConfig, projection);
        }
        else
        {
            var projection = BuildSelectProjection(command.SelectColumns);
            selectQuery = query.Select(DynamicParsingConfig, projection);
        }

        if (!string.IsNullOrWhiteSpace(command.OrderByClause))
        {
            var orderProjection = BuildOrderByProjection(command.OrderByClause);
            selectQuery = selectQuery.OrderBy(DynamicParsingConfig, orderProjection);
        }

        var materialized = selectQuery.Cast<object>().ToList();
        return sqlExecutionResultObject.FromRows(materialized);
    }

    /// <summary>
    /// Materializes row contexts for the FROM table and sequential JOIN clauses.
    /// </summary>
    private List<IDictionary<string, object?>> BuildJoinContexts(string databaseName, SelectCommand command)
    {
        var mainTable = ResolveTable(databaseName, command.From.Table);
        var baseRows = mainTable.GetRows().Cast<object?>().ToList();

        // Each context is an expando map keyed by table alias pointing to the current row.
        var initialContexts = new List<IDictionary<string, object?>>(baseRows.Count);
        foreach (var row in baseRows)
            initialContexts.Add(CreateContext(command.From.Alias, row));

        var currentContexts = initialContexts;

        foreach (var join in command.Joins)
        {
            var rightTable = ResolveTable(databaseName, join.Table);
            var rightRows = rightTable.GetRows().Cast<object?>();
            var (rightLookup, nullBucket) = BuildJoinLookup(rightRows, join.RightColumn);

            var updatedContexts = new List<IDictionary<string, object?>>(currentContexts.Count);

            foreach (var ctx in currentContexts)
            {
                var leftValue = GetValue(ctx[join.LeftAlias], join.LeftColumn);

                if (!TryGetMatches(rightLookup, nullBucket, leftValue, out var matches))
                {
                    if (join.JoinType == JoinType.Left)
                        updatedContexts.Add(CloneWith(ctx, join.Alias, null));

                    continue;
                }

                foreach (var match in matches)
                    updatedContexts.Add(CloneWith(ctx, join.Alias, match));
            }

            currentContexts = updatedContexts;
        }

        return currentContexts;
    }

    private (Dictionary<object, List<object?>>, List<object?>?) BuildJoinLookup(
        IEnumerable<object?> rows,
        string columnName)
    {
        var lookup = new Dictionary<object, List<object?>>();
        List<object?>? nullBucket = null;

        foreach (var row in rows)
        {
            var key = GetValue(row, columnName);
            if (key is null)
            {
                (nullBucket ??= new List<object?>()).Add(row);
                continue;
            }

            if (!lookup.TryGetValue(key, out var bucket))
            {
                bucket = new List<object?>();
                lookup[key] = bucket;
            }

            bucket.Add(row);
        }

        return (lookup, nullBucket);
    }

    private bool TryGetMatches(
        Dictionary<object, List<object?>> lookup,
        List<object?>? nullBucket,
        object? key,
        out IReadOnlyList<object?> matches)
    {
        if (key is null)
        {
            if (nullBucket is not null && nullBucket.Count > 0)
            {
                matches = nullBucket;
                return true;
            }

            matches = Array.Empty<object?>();
            return false;
        }

        if (lookup.TryGetValue(key, out var bucket) && bucket.Count > 0)
        {
            matches = bucket;
            return true;
        }

        matches = Array.Empty<object?>();
        return false;
    }

    /// <summary>
    /// Projects non-aggregated SELECT expressions into a dynamic anonymous type.
    /// </summary>
    private string BuildSelectProjection(IReadOnlyList<SelectColumn> columns)
        => "new (" + string.Join(", ", columns.Select(BuildSelectExpression)) + ")";

    /// <summary>
    /// Projects grouped results, applying aggregate functions and surfacing GROUP BY keys.
    /// </summary>
    private string BuildGroupedSelectProjection(
        IReadOnlyList<SelectColumn> columns,
        IReadOnlyList<GroupByColumn> groupColumns)
    {
        return "new (" + string.Join(", ", columns.Select(c =>
        {
            if (c.Aggregate != AggregateType.None)
            {
                var sourceExpression = NormalizeValueExpression(c.Expression, allowAggregates: true);
                return c.Aggregate switch
                {
                    AggregateType.Count when sourceExpression == "*" =>
                        $"it.Count() as {c.OutputName}",
                    AggregateType.Count =>
                        $"it.Count(item => {BuildGroupItemExpression(sourceExpression)}) as {c.OutputName}",
                    AggregateType.Sum =>
                        $"it.Sum(item => {BuildGroupItemExpression(sourceExpression)}) as {c.OutputName}",
                    AggregateType.Avg =>
                        $"it.Average(item => {BuildGroupItemExpression(sourceExpression)}) as {c.OutputName}",
                    AggregateType.Min =>
                        $"it.Min(item => {BuildGroupItemExpression(sourceExpression)}) as {c.OutputName}",
                    AggregateType.Max =>
                        $"it.Max(item => {BuildGroupItemExpression(sourceExpression)}) as {c.OutputName}",
                    _ => throw new NotSupportedException("Unsupported aggregate.")
                };
            }

            var matchingGroup = groupColumns.FirstOrDefault(g =>
                string.Equals(g.OriginalExpression, c.Expression, StringComparison.OrdinalIgnoreCase));

            if (matchingGroup == null)
                throw new InvalidOperationException($"Column '{c.Expression}' must either be aggregated or appear in GROUP BY.");

            return $"it.Key.{matchingGroup.ProjectedName} as {c.OutputName}";
        })) + ")";
    }

    /// <summary>
    /// Translates ORDER BY clauses into Dynamic LINQ order instructions.
    /// </summary>
    private string BuildOrderByProjection(string orderClause)
    {
        var parts = SplitCsv(orderClause).Select(part =>
        {
            var trimmed = part.Trim();
            var descending = trimmed.EndsWith(" DESC", StringComparison.OrdinalIgnoreCase);
            var ascending = trimmed.EndsWith(" ASC", StringComparison.OrdinalIgnoreCase);

            var expression = descending
                ? trimmed[..^5]
                : ascending
                    ? trimmed[..^4]
                    : trimmed;

            var normalized = NormalizeValueExpression(expression);
            return descending ? $"{normalized} descending" : normalized;
        });

        return string.Join(", ", parts);
    }

    /// <summary>
    /// Normalizes a single SELECT column into a Dynamic LINQ projection and alias.
    /// </summary>
    private string BuildSelectExpression(SelectColumn column)
    {
        var normalized = NormalizeValueExpression(column.Expression);

        return column.Aggregate switch
        {
            AggregateType.None => $"{normalized} as {column.OutputName}",
            AggregateType.Count when normalized == "*" => $"Count() as {column.OutputName}",
            AggregateType.Count => $"Count(item => {BuildGroupItemExpression(normalized)}) as {column.OutputName}",
            AggregateType.Sum => $"Sum(item => {BuildGroupItemExpression(normalized)}) as {column.OutputName}",
            AggregateType.Avg => $"Average(item => {BuildGroupItemExpression(normalized)}) as {column.OutputName}",
            AggregateType.Min => $"Min(item => {BuildGroupItemExpression(normalized)}) as {column.OutputName}",
            AggregateType.Max => $"Max(item => {BuildGroupItemExpression(normalized)}) as {column.OutputName}",
            _ => throw new InvalidOperationException($"Unsupported aggregate function '{column.RawExpression}'.")
        };
    }

    /// <summary>
    /// Builds the anonymous type used as the grouping key for GROUP BY operations.
    /// </summary>
    private string BuildGroupKeyProjection(IReadOnlyList<GroupByColumn> columns)
    {
        return "new (" + string.Join(", ",
            columns.Select(c => $"{NormalizeValueExpression(c.OriginalExpression)} as {c.ProjectedName}")) + ")";
    }

    // ---------------------- INSERT ----------------------

    /// <summary>
    /// Creates a new row instance via reflection and populates it with INSERT literals.
    /// </summary>
    private SqlExecutionResult ExecuteInsert(string databaseName, InsertCommand command)
    {
        var table = ResolveTable(databaseName, command.TableName);
        var rowType = table.DataType;
        var columns = command.Columns;

        var instance = Activator.CreateInstance(rowType)
                      ?? throw new InvalidOperationException($"Type '{rowType}' requires a parameterless constructor.");

        for (var i = 0; i < columns.Count; i++)
        {
            var property = rowType.GetProperty(columns[i], BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)
                           ?? throw new InvalidOperationException($"Column '{columns[i]}' not found on type '{rowType.Name}'.");

            var value = ConvertLiteral(command.Values[i], property.PropertyType);
            property.SetValue(instance, value);
        }

        table.Add(instance!);
        return sqlExecutionResultObject.FromNonQuery(1);
    }

    // ---------------------- UPDATE ----------------------

    /// <summary>
    /// Applies UPDATE assignments to rows that satisfy an optional predicate.
    /// </summary>
    private SqlExecutionResult ExecuteUpdate(string databaseName, UpdateCommand command)
    {
        var table = ResolveTable(databaseName, command.TableName);
        var rowType = table.DataType;

        var predicate = BuildPredicate(rowType, command.WhereClause);
        var assignments = command.Assignments
            .Select(a =>
            {
                var property = rowType.GetProperty(a.Column, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase)
                               ?? throw new InvalidOperationException($"Column '{a.Column}' not found on '{rowType.Name}'.");
                var value = ConvertLiteral(a.Value, property.PropertyType);
                return (Property: property, Value: value);
            })
            .ToList();

        var affected = table.Update(
            predicate,
            row =>
            {
                foreach (var assignment in assignments)
                {
                    assignment.Property.SetValue(row, assignment.Value);
                }
            });

        return sqlExecutionResultObject.FromNonQuery(affected);
    }

    // ---------------------- DELETE ----------------------

    /// <summary>
    /// Removes rows that satisfy the WHERE predicate while keeping unmatched rows.
    /// </summary>
    private SqlExecutionResult ExecuteDelete(string databaseName, DeleteCommand command)
    {
        var table = ResolveTable(databaseName, command.TableName);
        var rowType = table.DataType;

        var predicate = BuildPredicate(rowType, command.WhereClause);
        var affected = table.Delete(predicate);

        return sqlExecutionResultObject.FromNonQuery(affected);
    }

    // ---------------------- Parsing Helpers ----------------------

    /// <summary>
    /// Parses a SELECT statement into its component parts for later execution.
    /// </summary>
    private SelectCommand ParseSelect(string sql)
    {
        var normalized = Regex.Replace(sql, @"\s+", " ").Trim();

        var selectMatch = Regex.Match(normalized, @"SELECT (.+?) FROM (.+)", RegexOptions.IgnoreCase);
        if (!selectMatch.Success)
            throw new InvalidOperationException("Malformed SELECT statement.");

        var selectPart = selectMatch.Groups[1].Value.Trim();
        var fromAndRest = selectMatch.Groups[2].Value.Trim();

        var fromMatch = Regex.Match(fromAndRest, @"^(?<table>[A-Za-z_][\w\.]*)(?:\s+(?<alias>[A-Za-z_][\w]*))?(?<rest>.*)$",
            RegexOptions.IgnoreCase);
        if (!fromMatch.Success)
            throw new InvalidOperationException("Malformed FROM clause.");

        var from = new TableReference(
            fromMatch.Groups["table"].Value,
            fromMatch.Groups["alias"].Success
                ? fromMatch.Groups["alias"].Value
                : fromMatch.Groups["table"].Value);

        var rest = fromMatch.Groups["rest"].Value;

        var joins = ParseJoins(ref rest);
        var where = ExtractClause(ref rest, "WHERE");
        var groupBy = ExtractClause(ref rest, "GROUP BY");
        var orderBy = ExtractClause(ref rest, "ORDER BY");

        return new SelectCommand(
            ParseSelectColumns(selectPart),
            from,
            joins,
            where,
            ParseGroupBy(groupBy),
            orderBy);
    }

    /// <summary>
    /// Splits and normalizes the raw SELECT list into structured column definitions.
    /// </summary>
    private IReadOnlyList<SelectColumn> ParseSelectColumns(string selectPart)
    {
        var parts = SplitCsv(selectPart);
        var result = new List<SelectColumn>(parts.Count);

        foreach (var part in parts)
        {
            var trimmed = part.Trim();
            var alias = ParseAlias(trimmed, out var expressionWithoutAlias);
            var aggregate = DetectAggregate(expressionWithoutAlias, out var normalizedExpression);

            result.Add(new SelectColumn(trimmed, normalizedExpression, alias, aggregate));
        }

        return result;
    }

    /// <summary>
    /// Parses the GROUP BY clause into reusable column descriptors.
    /// </summary>
    private IReadOnlyList<GroupByColumn> ParseGroupBy(string? groupBy)
    {
        if (string.IsNullOrWhiteSpace(groupBy))
            return Array.Empty<GroupByColumn>();

        var expressions = SplitCsv(groupBy);
        return expressions
            .Select(expr => new GroupByColumn(expr.Trim()))
            .ToList();
    }

    /// <summary>
    /// Iteratively extracts JOIN clauses from the remaining SQL text.
    /// </summary>
    private IReadOnlyList<JoinClause> ParseJoins(ref string rest)
    {
        var result = new List<JoinClause>();
        while (true)
        {
            var match = Regex.Match(rest, @"\s*(JOIN|LEFT JOIN)\s+([A-Za-z_][\w\.]*)(?:\s+([A-Za-z_][\w]*))?\s+ON\s+(.+?)(?=(\s+JOIN|\s+LEFT JOIN|\s+WHERE|\s+GROUP BY|\s+ORDER BY|$))",
                RegexOptions.IgnoreCase);
            if (!match.Success)
                break;

            var joinType = match.Groups[1].Value.Equals("LEFT JOIN", StringComparison.OrdinalIgnoreCase)
                ? JoinType.Left
                : JoinType.Inner;

            var tableName = match.Groups[2].Value;
            var alias = match.Groups[3].Success ? match.Groups[3].Value : tableName;
            var onClause = match.Groups[4].Value.Trim();

            var equality = Regex.Match(onClause, @"([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\s*=\s*([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)",
                RegexOptions.IgnoreCase);
            if (!equality.Success)
                throw new InvalidOperationException($"Unsupported JOIN expression '{onClause}'.");

            result.Add(new JoinClause(
                joinType,
                tableName,
                alias,
                equality.Groups[1].Value,
                equality.Groups[2].Value,
                equality.Groups[3].Value,
                equality.Groups[4].Value));

            rest = rest.Substring(match.Length);
        }

        return result;
    }

    /// <summary>
    /// Parses INSERT statements requiring an explicit column list and value tuple.
    /// </summary>
    private InsertCommand ParseInsert(string sql)
    {
        var match = Regex.Match(sql,
            @"INSERT\s+INTO\s+([A-Za-z_][\w\.]*)\s*\((.+?)\)\s*VALUES\s*\((.+)\)",
            RegexOptions.IgnoreCase);
        if (!match.Success)
            throw new InvalidOperationException("Malformed INSERT statement. Provide explicit column list, e.g. INSERT INTO Table (Col1, Col2) VALUES (...).");

        return new InsertCommand(
            match.Groups[1].Value,
            SplitCsv(match.Groups[2].Value).Select(c => c.Trim()).ToList(),
            SplitCsv(match.Groups[3].Value).Select(v => v.Trim()).ToList());
    }

    /// <summary>
    /// Parses UPDATE assignments and optional WHERE clause.
    /// </summary>
    private UpdateCommand ParseUpdate(string sql)
    {
        var match = Regex.Match(sql,
            @"UPDATE\s+([A-Za-z_][\w\.]*)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$",
            RegexOptions.IgnoreCase);
        if (!match.Success)
            throw new InvalidOperationException("Malformed UPDATE statement.");

        var assignments = SplitCsv(match.Groups[2].Value)
            .Select(part =>
            {
                var assignmentMatch = Regex.Match(part, @"([A-Za-z_][\w]*)\s*=\s*(.+)");
                if (!assignmentMatch.Success)
                    throw new InvalidOperationException($"Malformed assignment '{part}'.");
                return new UpdateAssignment(assignmentMatch.Groups[1].Value.Trim(), assignmentMatch.Groups[2].Value.Trim());
            })
            .ToList();

        return new UpdateCommand(match.Groups[1].Value, assignments, match.Groups[3].Success ? match.Groups[3].Value.Trim() : null);
    }

    /// <summary>
    /// Parses DELETE statements including the optional filter expression.
    /// </summary>
    private DeleteCommand ParseDelete(string sql)
    {
        var match = Regex.Match(sql,
            @"DELETE\s+FROM\s+([A-Za-z_][\w\.]*)(?:\s+WHERE\s+(.+))?$",
            RegexOptions.IgnoreCase);
        if (!match.Success)
            throw new InvalidOperationException("Malformed DELETE statement.");

        return new DeleteCommand(match.Groups[1].Value, match.Groups[2].Success ? match.Groups[2].Value.Trim() : null);
    }

    /// <summary>
    /// Removes the specified clause from the SQL fragment and returns its contents if found.
    /// </summary>
    private string? ExtractClause(ref string source, string clause)
    {
        var regex = new Regex($@"\s+{clause}\s+(.+?)(?=(\s+WHERE|\s+GROUP BY|\s+ORDER BY|$))", RegexOptions.IgnoreCase);
        var match = regex.Match(source);
        if (!match.Success)
            return null;

        source = source.Remove(match.Index, match.Length);
        return match.Groups[1].Value.Trim();
    }

    /// <summary>
    /// Splits a comma-separated list while respecting parentheses nesting.
    /// </summary>
    private IReadOnlyList<string> SplitCsv(string input)
    {
        var parts = new List<string>();
        var buffer = "";
        var depth = 0;

        foreach (var ch in input)
        {
            if (ch == '(')
                depth++;
            if (ch == ')')
                depth--;

            if (ch == ',' && depth == 0)
            {
                parts.Add(buffer);
                buffer = "";
            }
            else
            {
                buffer += ch;
            }
        }

        if (!string.IsNullOrWhiteSpace(buffer))
            parts.Add(buffer);

        return parts;
    }

    /// <summary>
    /// Determines the output alias for a SELECT expression and returns the raw expression.
    /// </summary>
    private string ParseAlias(string expression, out string withoutAlias)
    {
        var match = Regex.Match(expression, @"(.+?)\s+AS\s+([A-Za-z_][\w]*)$", RegexOptions.IgnoreCase);
        if (match.Success)
        {
            withoutAlias = match.Groups[1].Value.Trim();
            return match.Groups[2].Value.Trim();
        }

        var tokens = expression.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length >= 2 && !IsReservedKeyword(tokens[^1]))
        {
            withoutAlias = string.Join(' ', tokens[..^1]);
            return tokens[^1];
        }

        withoutAlias = expression;
        return Regex.Replace(expression, @"[^\w]", "_");
    }

    /// <summary>
    /// Identifies aggregate functions and extracts the inner expression.
    /// </summary>
    private AggregateType DetectAggregate(string expression, out string normalized)
    {
        var trimmed = expression.Trim();
        var aggregateMatch = Regex.Match(trimmed, @"^(COUNT|SUM|AVG|MIN|MAX)\s*\((.*)\)$", RegexOptions.IgnoreCase);

        if (!aggregateMatch.Success)
        {
            normalized = trimmed;
            return AggregateType.None;
        }

        normalized = aggregateMatch.Groups[2].Value.Trim();
        return aggregateMatch.Groups[1].Value.ToUpperInvariant() switch
        {
            "COUNT" => AggregateType.Count,
            "SUM" => AggregateType.Sum,
            "AVG" => AggregateType.Avg,
            "MIN" => AggregateType.Min,
            "MAX" => AggregateType.Max,
            _ => AggregateType.None
        };
    }

    /// <summary>
    /// Treats ORDER BY direction tokens as reserved to prevent them from becoming aliases.
    /// </summary>
    private bool IsReservedKeyword(string value)
        => value.Equals("ASC", StringComparison.OrdinalIgnoreCase) ||
           value.Equals("DESC", StringComparison.OrdinalIgnoreCase);

    // ---------------------- Expression Helpers ----------------------

    /// <summary>
    /// Converts SQL boolean syntax into Dynamic LINQ compatible operators and literals.
    /// </summary>
    private string NormalizeBooleanExpression(string expression)
    {
        var normalized = expression;
        normalized = Regex.Replace(normalized, @"<>", "!=");
        normalized = Regex.Replace(normalized, @"(?<=[^<>!=])=(?=[^=])", "==");
        normalized = Regex.Replace(normalized, @"\bAND\b", "&&", RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"\bOR\b", "||", RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"\bIS\s+NOT\s+NULL\b", "!= null", RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"\bIS\s+NULL\b", "== null", RegexOptions.IgnoreCase);
        normalized = Regex.Replace(normalized, @"'([^']*)'", "\"$1\"");
        return NormalizeValueExpression(normalized);
    }

    /// <summary>
    /// Normalizes identifiers and literals for Dynamic LINQ consumption, optionally preserving aggregates.
    /// </summary>
    private string NormalizeValueExpression(string expression, bool allowAggregates = false)
    {
        var normalized = expression.Trim();
        normalized = Regex.Replace(normalized, @"\b([A-Za-z_][\w]*)\.([A-Za-z_][\w]*)\b", "$1.$2");
        normalized = Regex.Replace(normalized, @"'([^']*)'", "\"$1\"");

        if (!allowAggregates)
            normalized = Regex.Replace(normalized, @"\bCOUNT\s*\(\s*\*\s*\)\b", "Count()",
                RegexOptions.IgnoreCase);

        return normalized;
    }

    /// <summary>
    /// Ensures aggregate selector expressions address the grouped item when necessary.
    /// </summary>
    private string BuildGroupItemExpression(string expression)
        => Regex.IsMatch(expression, @"^[A-Za-z_][\w]*\.[A-Za-z_][\w]*$")
            ? $"item.{expression}"
            : expression;

    /// <summary>
    /// Compiles an optional WHERE clause into a predicate delegate for row evaluation.
    /// </summary>
    private Func<object, bool>? BuildPredicate(Type rowType, string? whereClause)
    {
        if (string.IsNullOrWhiteSpace(whereClause))
            return null;

        var normalized = NormalizeBooleanExpression(whereClause);
        var lambda = DynamicExpressionParser.ParseLambda(rowType, typeof(bool), normalized);
        var compiled = lambda.Compile();

        return row => (bool)compiled.DynamicInvoke(row)!;
    }

    private string ResolveDatabaseName(string? databaseName)
    {
        var resolved = string.IsNullOrWhiteSpace(databaseName) ? _defaultDatabaseName : databaseName?.Trim();
        if (string.IsNullOrWhiteSpace(resolved))
        {
            resolved = _databaseManager.Databases.FirstOrDefault()?.Name;
        }

        if (string.IsNullOrWhiteSpace(resolved))
            throw new InvalidOperationException("A target database name must be provided before executing SQL statements.");

        if (!_databaseManager.Exists(resolved))
            throw new InvalidOperationException($"Database '{resolved}' was not found.");

        return resolved;
    }

    private ITable ResolveTable(string databaseName, string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be empty.", nameof(tableName));

        var database = _databaseManager.GetDatabase(databaseName)
            ?? throw new InvalidOperationException($"Database '{databaseName}' was not found.");

        var normalized = tableName.Trim();
        var table = database.Tables.FirstOrDefault(t => string.Equals(t.Name, normalized, StringComparison.OrdinalIgnoreCase));

        return table ?? throw new InvalidOperationException($"Table '{tableName}' was not found in database '{databaseName}'.");
    }

    // ---------------------- Utility ----------------------

    /// <summary>
    /// Creates a shallow copy of the join context and assigns a new alias entry.
    /// </summary>
    private IDictionary<string, object?> CloneWith(IDictionary<string, object?> source, string key, object? value)
    {
        var expando = new ExpandoObject() as IDictionary<string, object?>;
        foreach (var kvp in source)
            expando![kvp.Key] = kvp.Value;

        AssignAliasRow(expando!, key, value);
        return expando!;
    }

    private IDictionary<string, object?> CreateContext(string alias, object? row)
    {
        var expando = new ExpandoObject() as IDictionary<string, object?>;
        AssignAliasRow(expando!, alias, row);
        return expando!;
    }

    private void AssignAliasRow(IDictionary<string, object?> context, string alias, object? row)
    {
        context[alias] = row;

        if (row is null)
            return;

        var properties = row.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        foreach (var property in properties)
        {
            if (!property.CanRead)
                continue;

            var propertyName = property.Name;
            if (ContainsKeyInsensitive(context, propertyName))
                continue;

            context[propertyName] = property.GetValue(row);
        }
    }

    private static bool ContainsKeyInsensitive(IDictionary<string, object?> context, string candidate)
    {
        foreach (var existing in context.Keys)
        {
            if (string.Equals(existing, candidate, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    /// <summary>
    /// Retrieves a property value using case-insensitive reflection to mimic SQL semantics.
    /// </summary>
    private object? GetValue(object? instance, string property)
    {
        if (instance is null)
            return null;

        if (string.IsNullOrWhiteSpace(property))
            throw new ArgumentException("Property name cannot be empty.", nameof(property));

        var type = instance.GetType();
        var accessors = PropertyAccessorCache.GetOrAdd(
            type,
             _ => new ConcurrentDictionary<string, Func<object, object?>>(StringComparer.OrdinalIgnoreCase));

        var trimmedProperty = property.Trim();
        var accessor = accessors.GetOrAdd(
            trimmedProperty,
            propName => CreatePropertyAccessor(type, propName));

        return accessor(instance);
    }

    private Func<object, object?> CreatePropertyAccessor(Type type, string property)
    {
        var prop = type.GetProperty(property, BindingFlags.Public | BindingFlags.Instance | BindingFlags.IgnoreCase);
        if (prop == null)
            throw new InvalidOperationException($"Property '{property}' not found on '{type.Name}'.");

        var instanceParameter = Expression.Parameter(typeof(object), "instance");
        var typedInstance = Expression.Convert(instanceParameter, type);
        var propertyAccess = Expression.Property(typedInstance, prop);
        var boxedResult = Expression.Convert(propertyAccess, typeof(object));

        var accessor = Expression.Lambda<Func<object, object?>>(boxedResult, instanceParameter).Compile();
        return accessor;
    }

    /// <summary>
    /// Converts SQL literals into strongly-typed .NET values, honoring nullability.
    /// </summary>
    private object? ConvertLiteral(string literal, Type targetType)
    {
        if (literal.Equals("NULL", StringComparison.OrdinalIgnoreCase))
            return null;

        targetType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (targetType == typeof(string))
            return Regex.Replace(literal, @"^'(.*)'$", "$1").Replace("''", "'");

        if (targetType == typeof(bool))
            return bool.Parse(literal.Trim('\'', '"'));

        if (targetType == typeof(Guid))
            return Guid.Parse(literal.Trim('\'', '"'));

        if (targetType == typeof(DateTime))
            return DateTime.Parse(literal.Trim('\'', '"'), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);

        if (targetType.IsEnum)
            return Enum.Parse(targetType, literal.Trim('\'', '"'), ignoreCase: true);

        if (targetType == typeof(byte[]))
            return Convert.FromBase64String(literal.Trim('\'', '"'));

        return Convert.ChangeType(literal.Trim('\'', '"'), targetType, CultureInfo.InvariantCulture);
    }
    #endregion


    // ---------------------- DTOs ----------------------

    /// <summary>
    /// Represents the parsed shape of a SELECT statement.
    /// </summary>
    private sealed record SelectCommand(
        IReadOnlyList<SelectColumn> SelectColumns,
        TableReference From,
        IReadOnlyList<JoinClause> Joins,
        string? WhereClause,
        IReadOnlyList<GroupByColumn> GroupByColumns,
        string? OrderByClause);

    /// <summary>
    /// Describes a SELECT column including its alias and aggregate metadata.
    /// </summary>
    private sealed record SelectColumn(
        string RawExpression,
        string Expression,
        string OutputName,
        AggregateType Aggregate);

    /// <summary>
    /// Holds both the raw GROUP BY expression and its sanitized alias.
    /// </summary>
    private sealed class GroupByColumn
    {
        public string OriginalExpression { get; }
        public string ProjectedName { get; }

        public GroupByColumn(string expression)
        {
            OriginalExpression = expression;
            ProjectedName = Regex.Replace(expression, @"[^\w]", "_");
        }
    }

    /// <summary>
    /// Captures a table name and its working alias within a query.
    /// </summary>
    private sealed record TableReference(string Table, string Alias);

    /// <summary>
    /// Encapsulates a JOIN relationship between two aliases.
    /// </summary>
    private sealed record JoinClause(
        JoinType JoinType,
        string Table,
        string Alias,
        string LeftAlias,
        string LeftColumn,
        string RightAlias,
        string RightColumn);

    /// <summary>
    /// Represents the target table, columns, and values of an INSERT command.
    /// </summary>
    private sealed record InsertCommand(
        string TableName,
        IReadOnlyList<string> Columns,
        IReadOnlyList<string> Values);

    /// <summary>
    /// Describes the table, assignments, and optional filter of an UPDATE command.
    /// </summary>
    private sealed record UpdateCommand(
        string TableName,
        IReadOnlyList<UpdateAssignment> Assignments,
        string? WhereClause);

    /// <summary>
    /// Pairs a column name with the raw SQL expression assigned to it.
    /// </summary>
    private sealed record UpdateAssignment(string Column, string Value);

    /// <summary>
    /// Represents the target table and optional filter for a DELETE command.
    /// </summary>
    private sealed record DeleteCommand(string TableName, string? WhereClause);

    /// <summary>
    /// Supported JOIN types in the simplified SQL grammar.
    /// </summary>
    private enum JoinType { Inner, Left }

    /// <summary>
    /// Aggregate functions recognized during SELECT parsing.
    /// </summary>
    private enum AggregateType { None, Count, Sum, Avg, Min, Max }
}

public class SqlExecutionResult
{
    private SqlExecutionResult(IReadOnlyList<object>? rows, int affectedRows, bool isQuery)
    {
        Rows = rows;
        AffectedRows = affectedRows;
        IsQuery = isQuery;
    }

    public SqlExecutionResult()
    {
    }

    public IReadOnlyList<object>? Rows { get; }
    public int AffectedRows { get; }
    public bool IsQuery { get; }

    public SqlExecutionResult FromRows(IReadOnlyList<object> rows)
        => new(rows, rows.Count, true);

    public SqlExecutionResult FromNonQuery(int affectedRows)
        => new(null, affectedRows, false);
}
