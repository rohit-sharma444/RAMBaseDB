namespace RAMBaseDB.Domain.ValueObjects;

using System.Collections.Generic;
using System.Text.Json;

/// <summary>
/// Represents the serialized payload for a table snapshot (row type metadata + rows).
/// </summary>
public sealed class TableData
{
    public string TypeName { get; set; } = string.Empty;
    public List<JsonElement> Rows { get; set; } = [];
}
