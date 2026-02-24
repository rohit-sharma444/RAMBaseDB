namespace RAMBaseDB.Domain.Entities;

using RAMBaseDB.Domain.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;

/// <summary>
/// Represents a collection of tables, providing list-based access and name-based lookup helpers.
/// </summary>
public class Tables : List<ITable>, IDisposable
{
    public new void Add(ITable table)
    {
        ArgumentNullException.ThrowIfNull(table);
        base.Add(table);
    }

    public new void Insert(int index, ITable table)
    {
        ArgumentNullException.ThrowIfNull(table);
        base.Insert(index, table);
    }

    public new void AddRange(IEnumerable<ITable> tables)
        => base.AddRange(ValidateRange(tables));

    public new void InsertRange(int index, IEnumerable<ITable> tables)
        => base.InsertRange(index, ValidateRange(tables));

    public new ITable this[int index]
    {
        get => base[index];
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            base[index] = value;
        }
    }

    public ITable this[string tableName]
    {
        get => FindByName(tableName) ?? throw new KeyNotFoundException($"Table '{tableName}' not found.");
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            var normalized = NormalizeName(tableName);
            var index = FindIndex(table => string.Equals(table.Name, normalized, StringComparison.Ordinal));
            if (index >= 0)
            {
                base[index] = value;
                return;
            }

            Add(value);
        }
    }

    public void Dispose()
    {
        foreach (var table in this)
        {
            if (table is IDisposable disposable)
                disposable.Dispose();
        }

        Clear();
    }

    private static IEnumerable<ITable> ValidateRange(IEnumerable<ITable> tables)
    {
        ArgumentNullException.ThrowIfNull(tables);

        foreach (var table in tables)
        {
            ArgumentNullException.ThrowIfNull(table);
            yield return table;
        }
    }

    private static string NormalizeName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be empty.", nameof(tableName));

        return tableName.Trim();
    }

    private ITable? FindByName(string tableName)
    {
        var normalized = NormalizeName(tableName);
        return this.FirstOrDefault(table => string.Equals(table.Name, normalized, StringComparison.Ordinal));
    }
}
