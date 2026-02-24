namespace RAMBaseDB.Domain.Entities;

using System;

/// <summary>
/// Represents an in-memory database containing a collection of tables.
/// </summary>
/// <remarks>The Database class manages the lifecycle of its tables and provides access to them through the Tables
/// property. Disposing the Database instance will dispose all contained tables and clear the collection. This class is
/// not thread-safe; concurrent access should be synchronized externally if required.</remarks>
public sealed class Database : IDisposable
{
    private bool _disposed;

    public Database() => Tables = [];

    public string Name { get; set; } = string.Empty;

    public Tables Tables { get; set;}

    public void Dispose()
    {
        if (_disposed)
            return;

        // Only dispose tables if they implement IDisposable
        foreach (var table in Tables)
        {
            if (table is IDisposable disposable)
                disposable.Dispose();
        }

        Tables.Clear();
        _disposed = true;
    }
}
