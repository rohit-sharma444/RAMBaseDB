namespace RAMBaseDB.Domain.Schema;

using RAMBaseDB.Domain.Abstractions;
using System;
using System.Collections.Generic;

internal static class TableRegistry
{
    private static readonly Dictionary<Type, WeakReference<IForeignKeyTargetTable>> _tables = new();
    private static readonly Dictionary<Type, List<WeakReference<IForeignKeyDependentTable>>> _dependents = new();
    private static readonly object _sync = new();

    public static void Register(IForeignKeyTargetTable table)
    {
        ArgumentNullException.ThrowIfNull(table);
        lock (_sync)
        {
            _tables[table.EntityType] = new WeakReference<IForeignKeyTargetTable>(table);
        }
    }

    public static bool TryGet(Type entityType, out IForeignKeyTargetTable table)
    {
        ArgumentNullException.ThrowIfNull(entityType);

        lock (_sync)
        {
            if (_tables.TryGetValue(entityType, out var weak) && weak.TryGetTarget(out var target))
            {
                table = target;
                return true;
            }

            _tables.Remove(entityType);
        }

        table = null;
        return false;
    }

    public static void RegisterDependency(Type targetType, IForeignKeyDependentTable dependent)
    {
        ArgumentNullException.ThrowIfNull(targetType);
        ArgumentNullException.ThrowIfNull(dependent);

        lock (_sync)
        {
            if (!_dependents.TryGetValue(targetType, out var list))
            {
                list = [];
                _dependents[targetType] = list;
            }

            list.Add(new WeakReference<IForeignKeyDependentTable>(dependent));
        }
    }

    public static IReadOnlyList<IForeignKeyDependentTable> GetDependents(Type targetType)
    {
        ArgumentNullException.ThrowIfNull(targetType);

        lock (_sync)
        {
            if (!_dependents.TryGetValue(targetType, out var list) || list.Count == 0)
                return Array.Empty<IForeignKeyDependentTable>();

            var result = new List<IForeignKeyDependentTable>(list.Count);
            for (var i = list.Count - 1; i >= 0; i--)
            {
                if (list[i].TryGetTarget(out var dependent))
                {
                    result.Add(dependent);
                    continue;
                }

                list.RemoveAt(i);
            }

            return result;
        }
    }
}
