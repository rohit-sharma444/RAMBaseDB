namespace RAMBaseDB.Infrastructure.Configuration;

using System;
using System.IO;

/// <summary>
/// Application-wide directories and behaviors required to bootstrap a RAMBaseDB workspace.
/// </summary>
public sealed class WorkspaceOptions
{
    public string DefaultDatabaseName { get; set; } = "default";
    public string SnapshotDirectory { get; set; } = Path.Combine(Directory.GetCurrentDirectory(), "Snapshots");
    public string ConfigurationDirectory { get; set; } = Path.Combine(Directory.GetCurrentDirectory(), "Config");
    public string MetadataDirectory { get; set; } = Path.Combine(Directory.GetCurrentDirectory(), "Metadata");
    public string UsersDirectory { get; set; } = Path.Combine(Directory.GetCurrentDirectory(), "Users");
    public string DumpFilePrefix { get; set; } = "dump";
    public TimeSpan SnapshotInterval { get; set; } = TimeSpan.FromSeconds(900);
    public int MaxSnapshotHistory { get; set; } = 10;
    public bool EnableAutomaticSnapshots { get; set; } = true;
    public bool AutoRestoreLatestDump { get; set; } = true;
    public bool BootstrapMetadata { get; set; } = true;
    public int InsightSnapshotDepth { get; set; } = 5;

    public void Normalize()
    {
        DefaultDatabaseName = NormalizeName(DefaultDatabaseName, "default");
        SnapshotDirectory = NormalizeDirectory(SnapshotDirectory, "Snapshots");
        ConfigurationDirectory = NormalizeDirectory(ConfigurationDirectory, "Config");
        MetadataDirectory = NormalizeDirectory(MetadataDirectory, "Metadata");
        UsersDirectory = NormalizeDirectory(UsersDirectory, "Users");
        DumpFilePrefix = NormalizeName(DumpFilePrefix, "dump");
        SnapshotInterval = SnapshotInterval <= TimeSpan.Zero
            ? TimeSpan.FromSeconds(900)
            : SnapshotInterval;
        MaxSnapshotHistory = Math.Max(1, MaxSnapshotHistory);
        InsightSnapshotDepth = Math.Max(1, InsightSnapshotDepth);
    }

    public void EnsureDirectories()
    {
        Directory.CreateDirectory(SnapshotDirectory);
        Directory.CreateDirectory(ConfigurationDirectory);
        Directory.CreateDirectory(MetadataDirectory);
        Directory.CreateDirectory(UsersDirectory);
    }

    private static string NormalizeName(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
            return fallback;

        var buffer = value.Trim();
        foreach (var invalid in Path.GetInvalidFileNameChars())
            buffer = buffer.Replace(invalid, '-');

        return buffer.Length == 0 ? fallback : buffer;
    }

    private static string NormalizeDirectory(string? path, string defaultFolderName)
    {
        var candidate = string.IsNullOrWhiteSpace(path)
            ? Path.Combine(Directory.GetCurrentDirectory(), defaultFolderName)
            : path.Trim();

        var expanded = Environment.ExpandEnvironmentVariables(candidate);
        return Path.GetFullPath(expanded);
    }
}
