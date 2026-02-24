namespace RAMBaseDB.Domain.Schema;

using RAMBaseDB.Domain.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

/// <summary>
/// Represents the schema definition for a database table mapped to the specified entity type, including its columns and
/// primary key information.
/// </summary>
/// <remarks>Use this class to inspect the table name, columns, and primary key for an entity type when working
/// with database mapping or schema generation. The table name defaults to the entity type name if not specified. Only
/// one auto-increment column is allowed, and it must be the primary key.</remarks>
/// <typeparam name="T">The type of the entity that the table structure describes. Each public instance property of this type is mapped to a
/// table column.</typeparam>
public class TableStructure<T> : ITableStructure
{
    public Type EntityType { get; } = typeof(T);
    public string TableName { get; }
    public IReadOnlyList<ColumnInfo> Columns { get; }
    public ColumnInfo? PrimaryKey => Columns.FirstOrDefault(c => c.IsPrimaryKey);
    public IReadOnlyList<ColumnInfo> RequiredColumns { get; }
    public IReadOnlyList<ColumnInfo> ForeignKeyColumns { get; }

    public TableStructure()
        : this(null)
    {
    }

    public TableStructure(string? tableName = null)
    {
        TableName = tableName ?? typeof(T).Name;
        Columns = typeof(T)
        .GetProperties(BindingFlags.Public | BindingFlags.Instance)
        .Select(p => new ColumnInfo(p))
        .ToList();
        RequiredColumns = Columns.Where(c => c.IsRequired).ToList();
        ForeignKeyColumns = Columns.Where(c => c.IsForeignKey).ToList();

        // Validate: if auto-increment present then it must be primary key and integer-like
        var auto = Columns.Where(c => c.IsAutoIncrement).ToList();
        if (auto.Count > 1)
            throw new InvalidOperationException("Only one AutoIncrement column allowed.");

        if (auto.Count == 1 && !auto[0].IsPrimaryKey)
            throw new InvalidOperationException("AutoIncrement column must be a Primary Key.");
    }
}
