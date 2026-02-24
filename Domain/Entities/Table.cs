namespace RAMBaseDB.Domain.Entities;

using RAMBaseDB.Domain.Abstractions;
using RAMBaseDB.Domain.Schema;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;

/// <summary>
/// Represents an in-memory table for storing and managing entities of type <typeparamref name="T"/> with support for
/// primary key, foreign key, and required field constraints.
/// </summary>
/// <remarks>Table<T> provides thread-safe operations for inserting, querying, updating, and deleting entities,
/// enforcing relational constraints similar to a database table. Primary key uniqueness, required fields, and
/// referential integrity for foreign keys are validated on insert, update, and delete operations. All returned entities
/// are clones of the stored data to prevent unintended modifications. This class is suitable for scenarios such as unit
/// testing, prototyping, or lightweight in-memory data modeling where relational integrity is required but persistence
/// is not. Table instances are automatically registered for cross-table foreign key checks.</remarks>
/// <typeparam name="T">The type of entity stored in the table. Must be a reference type with a public parameterless constructor.</typeparam>
public class Table<T> : ITable where T : class, new()
{
    private readonly List<T> _rows = [];
    private readonly ReaderWriterLockSlim _lock = new(LockRecursionPolicy.NoRecursion);
    private int _nextAuto = 1;
    private readonly Dictionary<object, T> _primaryKeyIndex = new();

    // Static registry so tables can find each other for FK checks.
    private static readonly ConcurrentDictionary<Type, object> s_tables = TableInstanceRegistry.Tables;
    private static readonly ConcurrentDictionary<Type, ITableStructure> s_structureCache = new();

    // Metadata cached per-type
    private static readonly TableStructure<T> s_structure = new();
    private static readonly ColumnInfo? s_primaryKeyColumn = s_structure.PrimaryKey;
    private static readonly bool s_primaryKeyAuto = s_primaryKeyColumn?.IsAutoIncrement ?? false;
    private static readonly IReadOnlyList<ColumnInfo> s_requiredColumns = s_structure.RequiredColumns;
    private static readonly IReadOnlyList<ColumnInfo> s_foreignKeyColumns = s_structure.ForeignKeyColumns;
    private static readonly PropertyInfo[] s_cloneableProperties = s_structure.Columns
        .Select(c => c.Property)
        .Where(p => p.CanRead && p.CanWrite)
        .ToArray();
    private static IReadOnlyList<ForeignKeyReference> s_referencingTableCache = Array.Empty<ForeignKeyReference>();
    private static int s_referencingTableCacheVersion = -1;
    private static readonly object s_referencingCacheGate = new();
    public string Name { get; set; } = typeof(T).Name;
    public Type DataType => typeof(T);

    static Table()
    {
        s_structureCache.TryAdd(typeof(T), s_structure);
    }

    public Table() : this(null)
    {
    }

    public Table(string? name)
    {
        Name = string.IsNullOrWhiteSpace(name) ? typeof(T).Name : name.Trim();
        TableInstanceRegistry.Register(typeof(T), this);
    }

    private static object? GetPrimaryKeyValue(T obj)
    {
        if (s_primaryKeyColumn == null) return null;
        return s_primaryKeyColumn.Property.GetValue(obj);
    }

    private static void SetPrimaryKeyValue(T obj, object? value)
    {
        if (s_primaryKeyColumn == null) return;
        s_primaryKeyColumn.Property.SetValue(obj, value);
    }

    private void TrackPrimaryKey(T row)
    {
        if (s_primaryKeyColumn == null) return;
        var key = GetPrimaryKeyValue(row);
        if (key == null) return;
        _primaryKeyIndex[key] = row;
    }

    private void RemovePrimaryKeyTracking(object? key)
    {
        if (s_primaryKeyColumn == null || key == null) return;
        _primaryKeyIndex.Remove(key);
    }

    private void ReplacePrimaryKeyTracking(object? oldKey, T row)
    {
        if (s_primaryKeyColumn == null) return;
        var newKey = GetPrimaryKeyValue(row);
        if (!PrimaryKeyEquals(oldKey, newKey))
        {
            RemovePrimaryKeyTracking(oldKey);
        }

        if (newKey != null)
        {
            _primaryKeyIndex[newKey] = row;
        }
    }

    private static T Clone(T source)
    {
        var clone = new T();
        foreach (var p in s_cloneableProperties)
        {
            p.SetValue(clone, p.GetValue(source));
        }

        return clone;
    }

    private void ValidateRequired(T candidate)
    {
        foreach (var column in s_requiredColumns)
        {
            var val = column.Property.GetValue(candidate) ?? throw new InvalidOperationException($"Field '{column.Name}' is required.");
            if (column.Property.PropertyType == typeof(string) && string.IsNullOrEmpty((string)val!))
            {
                throw new InvalidOperationException($"Field '{column.Name}' is required.");
            }
        }
    }

    private bool PrimaryKeyEquals(object? a, object? b)
    {
        if (a == null && b == null) return true;
        if (a == null || b == null) return false;
        return a.Equals(b);
    }

    private bool ExistsPrimaryKey(object? key)
    {
        if (s_primaryKeyColumn == null) return false;
        if (key == null)
        {
            return _rows.Any(r => PrimaryKeyEquals(GetPrimaryKeyValue(r), null));
        }

        return _primaryKeyIndex.ContainsKey(key);
    }

    private void EnsureForeignKeysExist(T candidate)
    {
        foreach (var column in s_foreignKeyColumns)
        {
            var referencedType = column.ForeignKeyReferencedType ?? throw new InvalidOperationException($"Foreign key '{column.Name}' is missing a referenced type.");
            var val = column.Property.GetValue(candidate) ?? throw new InvalidOperationException("Foreign key value is null.");
            if (!s_tables.TryGetValue(referencedType, out var parentTableObj))
            {
                throw new InvalidOperationException("Foreign key requires existing parent table. Foreign key check failed.");
            }

            // invoke FindByPrimaryKey on parent table using reflection
            var findMethod = parentTableObj.GetType().GetMethod("FindByPrimaryKey") ?? throw new InvalidOperationException("Foreign key check failed.");
            var parent = findMethod.Invoke(parentTableObj, [val]) ?? throw new InvalidOperationException("Foreign key constraint failed. Foreign key references missing parent.");
        }
    }

    private static IReadOnlyList<ForeignKeyReference> GetReferencingForeignKeyMetadata()
    {
        var version = TableInstanceRegistry.Version;
        if (version == Volatile.Read(ref s_referencingTableCacheVersion))
            return s_referencingTableCache;

        lock (s_referencingCacheGate)
        {
            if (version == s_referencingTableCacheVersion)
                return s_referencingTableCache;

            var target = typeof(T);
            var references = new List<ForeignKeyReference>();

            foreach (var entry in s_tables)
            {
                var entityType = entry.Key;
                if (entityType == target)
                    continue;

                var structure = GetStructureFor(entityType);
                foreach (var column in structure.ForeignKeyColumns)
                {
                    if (column.ForeignKeyReferencedType != target)
                        continue;

                    var tableInstance = entry.Value;
                    var tableRuntimeType = tableInstance?.GetType();
                    var rowsField = tableRuntimeType?.GetField("_rows", BindingFlags.NonPublic | BindingFlags.Instance);
                    references.Add(new ForeignKeyReference(entityType, column.Property, rowsField));
                }
            }

            s_referencingTableCache = references;
            Volatile.Write(ref s_referencingTableCacheVersion, version);
            return s_referencingTableCache;
        }
    }

    public void Clear()
    {
        ExecuteWithWriteLock(ClearUnsafe);
    }

    private void ClearUnsafe()
    {
        if (_rows.Count == 0)
            return;

        _rows.Clear();
        _primaryKeyIndex.Clear();
        if (s_primaryKeyColumn != null && s_primaryKeyAuto)
            _nextAuto = 1;
    }

    void ITable.Add(object item)
    {
        if (item is not T typed)
            throw new InvalidOperationException($"Row type '{item?.GetType().Name ?? "null"}' is not compatible with table '{Name}' expecting '{typeof(T).Name}'.");

        Insert(typed);
    }

    public void Insert(T source)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));
        var candidate = Clone(source);

        ExecuteWithWriteLock(() =>
        {
            // Handle auto-increment PK
            if (s_primaryKeyColumn != null && s_primaryKeyAuto)
            {
                var currentPkVal = s_primaryKeyColumn.Property.GetValue(candidate);
                if (currentPkVal is int preset && preset > 0)
                {
                    // advance next auto if needed
                    if (preset >= _nextAuto)
                    {
                        _nextAuto = preset + 1;
                    }
                }
                else
                {
                    // assign next auto
                    var assigned = Interlocked.Increment(ref _nextAuto) - 1;
                    s_primaryKeyColumn.Property.SetValue(candidate, assigned);
                }
            }
            else if (s_primaryKeyColumn != null)
            {
                // non auto PK must be set (not null or empty string)
                var pkVal = s_primaryKeyColumn.Property.GetValue(candidate);
                if (pkVal == null) throw new InvalidOperationException("Primary key must be provided.");
                if (pkVal is string s && string.IsNullOrEmpty(s)) throw new InvalidOperationException("Primary key must be provided.");
            }

            // validate required fields
            try
            {
                ValidateRequired(candidate);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException(ex.Message + " (required)", ex);
            }

            // foreign keys must refer to existing parent(s)
            try
            {
                EnsureForeignKeysExist(candidate);
            }
            catch (InvalidOperationException ex)
            {
                throw new InvalidOperationException("Foreign key: " + ex.Message, ex);
            }

            // enforce unique primary key
            var pk = GetPrimaryKeyValue(candidate);
            if (s_primaryKeyColumn != null && ExistsPrimaryKey(pk))
            {
                throw new InvalidOperationException("Duplicate primary key.");
            }

            // finally store the prepared candidate
            _rows.Add(candidate);
            TrackPrimaryKey(candidate);
        });
    }

    public void InsertRange(IEnumerable<T> items)
    {
        if (items == null) throw new ArgumentNullException(nameof(items));
        // Optimize by doing validation per item but hold lock while mutating
        var buffer = items.Select(Clone).ToList();

        ExecuteWithWriteLock(() =>
        {
            // First handle auto-increment presets and conflicts
            foreach (var candidate in buffer)
            {
                if (s_primaryKeyColumn != null && s_primaryKeyAuto)
                {
                    var currentPkVal = s_primaryKeyColumn.Property.GetValue(candidate);
                    if (currentPkVal is int preset && preset > 0)
                    {
                        if (preset >= _nextAuto) _nextAuto = preset + 1;
                    }
                }
            }

            // Now assign auto PKs
            foreach (var candidate in buffer)
            {
                if (s_primaryKeyColumn != null && s_primaryKeyAuto)
                {
                    var currentPkVal = s_primaryKeyColumn.Property.GetValue(candidate);
                    if (!(currentPkVal is int preset && preset > 0))
                    {
                        var assigned = Interlocked.Increment(ref _nextAuto) - 1;
                        s_primaryKeyColumn.Property.SetValue(candidate, assigned);
                    }
                }
            }

            // Validate and ensure uniqueness and foreign keys
            foreach (var candidate in buffer)
            {
                try
                {
                    ValidateRequired(candidate);
                }
                catch (InvalidOperationException ex)
                {
                    throw new InvalidOperationException(ex.Message + " (required)", ex);
                }

                try
                {
                    EnsureForeignKeysExist(candidate);
                }
                catch (InvalidOperationException ex)
                {
                    throw new InvalidOperationException("Foreign key: " + ex.Message, ex);
                }

                var pk = GetPrimaryKeyValue(candidate);
                if (s_primaryKeyColumn != null && ExistsPrimaryKey(pk))
                {
                    throw new InvalidOperationException("Duplicate primary key.");
                }

                _rows.Add(candidate);
                TrackPrimaryKey(candidate);
            }
        });
    }

    public IQueryable<T> AsQueryable()
        => ExecuteWithReadLock(() => _rows.Select(Clone).AsQueryable());

    public IEnumerable<T> Where(Func<T, bool> predicate)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        return ExecuteWithReadLock(() => _rows.Select(Clone).Where(predicate).ToList());
    }

    public List<T> ToList()
        => ExecuteWithReadLock(() => _rows.Select(Clone).ToList());

    public T? FindByPrimaryKey(object key)
    {
        if (s_primaryKeyColumn == null || key == null) return null;
        return ExecuteWithReadLock(() => _primaryKeyIndex.TryGetValue(key, out var found) ? Clone(found) : null);
    }

    public bool DeleteByPrimaryKey(object key)
    {
        if (s_primaryKeyColumn == null || key == null) return false;
        return ExecuteWithWriteLock(() =>
        {
            if (!_primaryKeyIndex.TryGetValue(key, out var row)) return false;
            var pkValue = s_primaryKeyColumn.Property.GetValue(row);

            // check referential integrity: ensure no other registered table references this primary key
            var referencingTables = GetReferencingForeignKeyMetadata();
            foreach (var reference in referencingTables)
            {
                if (!s_tables.TryGetValue(reference.EntityType, out var tableObj))
                    continue;

                var otherRows = SnapshotRows(reference, tableObj);
                if (otherRows is null)
                    continue;

                foreach (var other in otherRows)
                {
                    var val = reference.Property.GetValue(other);
                    if (PrimaryKeyEquals(val, pkValue))
                    {
                        throw new InvalidOperationException("Cannot delete row because it is referenced by other rows (referenced).");
                    }
                }
            }

            if (_rows.Remove(row))
            {
                RemovePrimaryKeyTracking(pkValue);
                return true;
            }

            return false;
        });
    }

    public int Delete(Func<T, bool> predicate)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        return ExecuteWithWriteLock(() =>
        {
            var toRemove = _rows.Where(r => predicate(Clone(r))).ToList();
            var referencingTables = s_primaryKeyColumn is not null
                ? GetReferencingForeignKeyMetadata()
                : Array.Empty<ForeignKeyReference>();
            int removed = 0;
            foreach (var r in toRemove)
            {
                // check referential integrity for each candidate
                var pkVal = s_primaryKeyColumn != null ? s_primaryKeyColumn.Property.GetValue(r) : null;
                if (s_primaryKeyColumn is not null)
                {
                    foreach (var reference in referencingTables)
                    {
                        if (!s_tables.TryGetValue(reference.EntityType, out var tableObj))
                            continue;

                        var otherRows = SnapshotRows(reference, tableObj);
                        if (otherRows is null)
                            continue;

                        foreach (var other in otherRows)
                        {
                            var val = reference.Property.GetValue(other);
                            if (PrimaryKeyEquals(val, pkVal))
                            {
                                throw new InvalidOperationException("Cannot delete row because it is referenced by other rows (referenced).");
                            }
                        }
                    }
                }

                if (_rows.Remove(r))
                {
                    RemovePrimaryKeyTracking(pkVal);
                    removed++;
                }
            }
            return removed;
        });
    }

    int ITable.Delete(Func<object, bool>? predicate)
    {
        if (predicate is null)
        {
            return ExecuteWithWriteLock(() =>
            {
                var removed = _rows.Count;
                ClearUnsafe();
                return removed;
            });
        }

        return Delete(row => predicate(row));
    }

    int ITable.Update(Func<object, bool>? predicate, Action<object> mutator)
    {
        ArgumentNullException.ThrowIfNull(mutator);

        Func<T, bool> typedPredicate = predicate is null
            ? _ => true
            : row => predicate(row);

        void TypedMutator(T row) => mutator(row);

        return Update(typedPredicate, TypedMutator);
    }

    public int Update(Func<T, bool> predicate, Action<T> mutator)
    {
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        if (mutator == null) throw new ArgumentNullException(nameof(mutator));

        var referencingTables = GetReferencingForeignKeyMetadata();

        return ExecuteWithWriteLock(() =>
        {
            var indices = _rows
                .Select((row, idx) => new { Row = row, Idx = idx })
                .Where(x => predicate(Clone(x.Row)))
                .ToList();

            int updated = 0;
            foreach (var item in indices)
            {
                var stored = item.Row;
                var candidate = Clone(stored);

                // apply mutation to candidate
                mutator(candidate);

                // If primary key changed, validate constraints
                var oldPk = s_primaryKeyColumn != null ? s_primaryKeyColumn.Property.GetValue(stored) : null;
                var newPk = s_primaryKeyColumn != null ? s_primaryKeyColumn.Property.GetValue(candidate) : null;

                // Validate required on candidate
                try
                {
                    ValidateRequired(candidate);
                }
                catch (InvalidOperationException ex)
                {
                    throw new InvalidOperationException(ex.Message + " (required)", ex);
                }

                // If this is a child with FK properties, ensure new FK values point to existing parent(s)
                try
                {
                    EnsureForeignKeysExist(candidate);
                }
                catch (InvalidOperationException ex)
                {
                    throw new InvalidOperationException("Foreign key: " + ex.Message, ex);
                }

                // Primary key changed?
                if (!PrimaryKeyEquals(oldPk, newPk))
                {
                    // If newPk collides with existing row -> fail
                    if (ExistsPrimaryKey(newPk))
                    {
                        throw new InvalidOperationException("Cannot change primary key to an existing value (primary key).");
                    }

                    // If this row is referenced by other tables, disallow changing parent PK
                    foreach (var reference in referencingTables)
                    {
                        if (!s_tables.TryGetValue(reference.EntityType, out var tableObj))
                            continue;

                        var otherRows = SnapshotRows(reference, tableObj);
                        if (otherRows is null)
                            continue;

                        foreach (var other in otherRows)
                        {
                            var val = reference.Property.GetValue(other);
                            if (PrimaryKeyEquals(val, oldPk))
                            {
                                throw new InvalidOperationException("Cannot change primary key while referenced by other rows (referenced).");
                            }
                        }
                    }
                }

                // Passed validations -> replace stored row
                var storedClone = Clone(candidate);
                _rows[item.Idx] = storedClone;
                ReplacePrimaryKeyTracking(oldPk, storedClone);
                updated++;
            }

            return updated;
        });
    }

    IEnumerable ITable.GetRows()
        => ExecuteWithReadLock(() => _rows.Select(Clone).ToList());

    private void ExecuteWithReadLock(Action action)
    {
        _lock.EnterReadLock();
        try
        {
            action();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private TResult ExecuteWithReadLock<TResult>(Func<TResult> action)
    {
        _lock.EnterReadLock();
        try
        {
            return action();
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private void ExecuteWithWriteLock(Action action)
    {
        _lock.EnterWriteLock();
        try
        {
            action();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private TResult ExecuteWithWriteLock<TResult>(Func<TResult> action)
    {
        _lock.EnterWriteLock();
        try
        {
            return action();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private static List<object>? SnapshotRows(ForeignKeyReference reference, object tableInstance)
    {
        if (reference.RowsField == null)
            return null;

        if (reference.RowsField.GetValue(tableInstance) is not System.Collections.IEnumerable enumerable)
            return null;

        var snapshot = new List<object>();
        foreach (var row in enumerable)
        {
            if (row is not null)
                snapshot.Add(row);
        }

        return snapshot;
    }

    private static ITableStructure GetStructureFor(Type entityType)
    {
        return s_structureCache.GetOrAdd(entityType, static type =>
        {
            var structureType = typeof(TableStructure<>).MakeGenericType(type);
            if (Activator.CreateInstance(structureType) is not ITableStructure structure)
            {
                throw new InvalidOperationException($"Unable to create table structure for type '{type.Name}'.");
            }

            return structure;
        });
    }

    private sealed class ForeignKeyReference
    {
        public ForeignKeyReference(Type entityType, PropertyInfo property, FieldInfo? rowsField)
        {
            EntityType = entityType;
            Property = property;
            RowsField = rowsField;
        }

        public Type EntityType { get; }
        public PropertyInfo Property { get; }
        public FieldInfo? RowsField { get; }
    }
}

internal static class TableInstanceRegistry
{
    private static int s_version;
    internal static readonly ConcurrentDictionary<Type, object> Tables = new();

    internal static int Version => Volatile.Read(ref s_version);

    internal static void Register(Type entityType, object table)
    {
        if (Tables.TryAdd(entityType, table))
        {
            Interlocked.Increment(ref s_version);
            return;
        }

        Tables[entityType] = table;
    }
}
