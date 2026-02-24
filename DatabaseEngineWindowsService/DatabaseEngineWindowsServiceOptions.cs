namespace DatabaseEngineWindowsService;

using Microsoft.Extensions.Hosting.WindowsServices;
using System;
using System.IO;

/// <summary>
/// Configuration settings that describe how the RAMBaseDB database engine should run as a Windows service.
/// </summary>
internal sealed class DatabaseEngineWindowsServiceOptions
{
    public const string SectionName = "DatabaseEngineService";

    /// <summary>
    /// Interval, in seconds, between forced snapshot persistence operations.
    /// </summary>
    public int DumpIntervalSeconds { get; set; } = 300;

    /// <summary>
    /// Directory where serialized and compressed snapshots will be stored.
    /// </summary>
    public string? SnapshotDirectory { get; set; }

    /// <summary>
    /// Directory containing persisted database configuration files.
    /// </summary>
    public string? ConfigurationDirectory { get; set; }

    /// <summary>
    /// Name of the database that the SQL parser should target by default.
    /// </summary>
    public string SqlDatabaseName { get; set; } = "default";

    public TimeSpan ResolveDumpInterval()
        => TimeSpan.FromSeconds(Math.Max(1, DumpIntervalSeconds <= 0 ? 30 : DumpIntervalSeconds));

    public string ResolveSnapshotDirectory()
    {
        var baseDirectory = ResolveWritableRoot();
        var configuredPath = SnapshotDirectory?.Trim();

        if (string.IsNullOrWhiteSpace(configuredPath))
            return Path.Combine(baseDirectory, "Snapshots");

        var expandedPath = Environment.ExpandEnvironmentVariables(configuredPath);
        if (Path.IsPathRooted(expandedPath))
            return expandedPath;

        return Path.GetFullPath(Path.Combine(baseDirectory, expandedPath));
    }

    public string ResolveConfigurationDirectory()
    {
        var baseDirectory = ResolveWritableRoot();
        var configuredPath = ConfigurationDirectory?.Trim();

        if (string.IsNullOrWhiteSpace(configuredPath))
            return Path.Combine(baseDirectory, "Config");

        var expandedPath = Environment.ExpandEnvironmentVariables(configuredPath);
        if (Path.IsPathRooted(expandedPath))
            return expandedPath;

        return Path.GetFullPath(Path.Combine(baseDirectory, expandedPath));
    }

    private static string ResolveWritableRoot()
    {
        if (OperatingSystem.IsWindows() && WindowsServiceHelpers.IsWindowsService())
        {
            var programData = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
            if (!string.IsNullOrWhiteSpace(programData))
                return Path.Combine(programData, "RAMBaseDB");
        }

        return AppContext.BaseDirectory;
    }
}
