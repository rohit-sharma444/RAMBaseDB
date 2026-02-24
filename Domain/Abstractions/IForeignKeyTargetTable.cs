namespace RAMBaseDB.Domain.Abstractions;

using System;

internal interface IForeignKeyTargetTable
{
    Type EntityType { get; }

    /// <summary>
    /// Determines whether the table currently contains a row with the provided primary key value.
    /// </summary>
    /// <param name="key">The key value to check. The implementation is responsible for normalizing the value.</param>
    /// <returns>True if a matching row exists; otherwise false.</returns>
    bool ContainsPrimaryKeyValue(object key);

    /// <summary>
    /// Normalizes an arbitrary key value into the CLR type used by the target table's primary key.
    /// </summary>
    /// <param name="key">The key value to normalize.</param>
    /// <returns>The normalized key.</returns>
    object NormalizeKey(object key);
}
