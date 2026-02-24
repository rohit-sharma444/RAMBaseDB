using RAMBaseDB.Domain.Schema;

namespace RAMBaseDB.Domain.Abstractions;

using System;
using System.Collections.Generic;

internal interface ITableStructure
{
    Type EntityType { get; }
    string TableName { get; }
    IReadOnlyList<ColumnInfo> Columns { get; }
    ColumnInfo? PrimaryKey { get; }
    IReadOnlyList<ColumnInfo> RequiredColumns { get; }
    IReadOnlyList<ColumnInfo> ForeignKeyColumns { get; }
}
