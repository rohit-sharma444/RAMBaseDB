namespace RAMBaseDB.Domain.Metadata;

using System.Collections.Generic;

/// <summary>
/// Summarizes the results of attempting to load schema metadata from disk.
/// </summary>
public sealed class MetadataLoadSummary
{
    public int DatabasesLoaded { get; internal set; }
    public int TablesLoaded { get; internal set; }
    public List<MetadataLoadError> Errors { get; } = new();
    public bool HasErrors => Errors.Count > 0;
}

/// <summary>
/// Captures a recoverable error that occurred while processing metadata files.
/// </summary>
public sealed class MetadataLoadError
{
    public string? DatabaseName { get; init; }
    public string? Resource { get; init; }
    public string Message { get; init; } = string.Empty;
}
