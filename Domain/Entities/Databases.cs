namespace RAMBaseDB.Domain.Entities;

using System;
using System.Collections.Generic;

/// <summary>
/// Represents a collection of database instances that can be individually managed and disposed.
/// </summary>
/// <remarks>This class extends <see cref="List{Database}"/> to provide collective management of multiple <see
/// cref="Database"/> objects. Disposing an instance of <see cref="Databases"/> will dispose all contained databases and
/// clear the collection. This class is sealed and cannot be inherited.</remarks>
public sealed class Databases : List<Database>, IDisposable
{
    public void Dispose()
    {
        foreach (var database in this)
            database?.Dispose();

        Clear();
    }
}
