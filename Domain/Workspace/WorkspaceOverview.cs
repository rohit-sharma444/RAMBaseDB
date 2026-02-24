namespace RAMBaseDB.Domain.Workspace;

using System;
using System.Collections.Generic;

/// <summary>
/// Snapshot of the current RAMBaseDB workspace, combining engine, filesystem, and configuration metadata.
/// </summary>
public sealed record WorkspaceOverview(
    string DefaultDatabase,
    int DatabaseCount,
    int TableCount,
    int SavedConfigurationCount,
    int SnapshotCount,
    long SnapshotFootprintBytes,
    DateTimeOffset? LatestSnapshotUtc,
    int UserDocumentCount,
    IReadOnlyList<DatabaseTableSummary> Databases,
    IReadOnlyList<SnapshotSummary> Snapshots,
    string SnapshotDirectory,
    string ConfigurationDirectory,
    string UsersDirectory,
    string MetadataDirectory);

public sealed record DatabaseTableSummary(string DatabaseName, int TableCount);

public sealed record SnapshotSummary(string FileName, DateTimeOffset LastWriteTimeUtc, long SizeBytes);
