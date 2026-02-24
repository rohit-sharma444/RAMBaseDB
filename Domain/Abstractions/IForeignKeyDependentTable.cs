namespace RAMBaseDB.Domain.Abstractions;

using System;

internal interface IForeignKeyDependentTable
{
    string TableName { get; }

    /// <summary>
    /// Determines whether this table currently has any rows referencing the specified target table and key.
    /// </summary>
    /// <param name="targetType">The entity type of the target table.</param>
    /// <param name="normalizedKey">The normalized primary key value of the target row.</param>
    /// <returns>True if a referencing row exists; otherwise false.</returns>
    bool HasReferenceTo(Type targetType, object normalizedKey);
}
