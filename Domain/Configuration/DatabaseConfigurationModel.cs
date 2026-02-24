namespace RAMBaseDB.Domain.Configuration;

using System;
using System.IO;

/// <summary>
/// Represents the configuration settings for a database instance, including file locations, snapshot behavior, and dump
/// management options.
/// </summary>
/// <remarks>Use this model to specify how the database should be registered, where configuration and dump files
/// are stored, and how automatic snapshot and restore features operate. The properties allow fine-grained control over
/// backup frequency, retention, and startup restore behavior. This type is typically used to initialize or update
/// database configuration in systems utilizing RAMBaseDB.</remarks>
public class DatabaseConfigurationModel
{
    private string _databaseName = string.Empty;
    private bool _databaseExists;
    private string _configurationDirectory = Path.Combine(Directory.GetCurrentDirectory(), "Config");
    private string _configurationFile = string.Empty;
    private string _dumpDirectory = Path.Combine(Directory.GetCurrentDirectory(), "Snapshots");
    private string _dumpFilePrefix = "dump";
    private bool _enableAutomaticSnapshots = true;
    private TimeSpan _snapshotInterval = TimeSpan.FromMinutes(5);
    private int _maxSnapshotHistory = 10;
    private bool _autoRestoreLatestDump;

    public string DatabaseName
    {
        get => _databaseName;
        set => _databaseName = value?.Trim() ?? string.Empty;
    }

    public bool DatabaseExists
    {
        get => _databaseExists;
        set => _databaseExists = value;
    }

    public string ConfigurationDirectory
    {
        get => _configurationDirectory;
        set => _configurationDirectory = NormalizeDirectory(value);
    }

    public string ConfigurationFile
    {
        get => string.IsNullOrWhiteSpace(_configurationFile)
            ? $"{DatabaseName}-config.json"
            : _configurationFile;
        set => _configurationFile = value?.Trim() ?? string.Empty;
    }

    public string DumpDirectory
    {
        get => _dumpDirectory;
        set => _dumpDirectory = NormalizeDirectory(value);
    }

    public string DumpFilePrefix
    {
        get => _dumpFilePrefix;
        set => _dumpFilePrefix = NormalizeFileName(value);
    }

    public bool EnableAutomaticSnapshots
    {
        get => _enableAutomaticSnapshots;
        set => _enableAutomaticSnapshots = value;
    }

    public TimeSpan SnapshotInterval
    {
        get => _snapshotInterval;
        set => _snapshotInterval = value <= TimeSpan.Zero
            ? TimeSpan.FromMinutes(5)
            : value;
    }

    public int MaxSnapshotHistory
    {
        get => _maxSnapshotHistory;
        set => _maxSnapshotHistory = Math.Max(1, value);
    }

    public bool AutoRestoreLatestDump
    {
        get => _autoRestoreLatestDump;
        set => _autoRestoreLatestDump = value;
    }

    private static string NormalizeDirectory(string? directoryPath)
    {
        var candidate = string.IsNullOrWhiteSpace(directoryPath)
            ? Directory.GetCurrentDirectory()
            : directoryPath.Trim();

        var expanded = Environment.ExpandEnvironmentVariables(candidate);
        return Path.GetFullPath(expanded);
    }

    private static string NormalizeFileName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return "dump";

        var result = value.Trim();
        foreach (var invalid in Path.GetInvalidFileNameChars())
            result = result.Replace(invalid, '_');

        return result.Length == 0 ? "dump" : result;
    }
}
