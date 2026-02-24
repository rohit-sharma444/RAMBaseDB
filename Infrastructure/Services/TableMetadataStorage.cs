namespace RAMBaseDB.Infrastructure.Services;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RAMBaseDB.Domain.Metadata;
using RAMBaseDB.Infrastructure.Configuration;

public interface ITableMetadataStorage
{
    Task<TableWriteResult> SaveTableAsync(
        string databaseName,
        TableMetadataDocument document,
        CancellationToken cancellationToken = default);

    Task<TableBatchWriteResult> ReplaceTablesAsync(
        string databaseName,
        IReadOnlyCollection<TableMetadataDocument> documents,
        CancellationToken cancellationToken = default);
}

public sealed record TableWriteResult(
    string DatabaseName,
    string TableName,
    string FilePath,
    bool Overwritten);

public sealed record TableBatchWriteResult(
    string DatabaseName,
    int TablesWritten,
    IReadOnlyList<string> WrittenFiles);

/// <summary>
/// Persists table metadata documents to the workspace Metadata directory.
/// </summary>
public sealed class FileSystemTableMetadataStorage : ITableMetadataStorage
{
    private static readonly JsonSerializerOptions SerializerOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true
    };

    private static readonly HashSet<char> InvalidFileNameCharacters = new(Path.GetInvalidFileNameChars());

    private readonly WorkspaceOptions _options;
    private readonly ILogger<FileSystemTableMetadataStorage> _logger;

    public FileSystemTableMetadataStorage(
        IOptions<WorkspaceOptions> options,
        ILogger<FileSystemTableMetadataStorage> logger)
    {
        _options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public async Task<TableWriteResult> SaveTableAsync(
        string databaseName,
        TableMetadataDocument document,
        CancellationToken cancellationToken = default)
    {
        if (document is null)
            throw new ArgumentNullException(nameof(document));

        var normalizedDatabase = NormalizeDatabaseName(databaseName);
        ValidateDocument(document);

        var directory = EnsureTablesDirectory(normalizedDatabase);
        var fileName = BuildStableFileName(document.TableName);
        var fullPath = Path.Combine(directory, fileName);
        var overwrote = File.Exists(fullPath);
        var payload = NormalizeDocument(document, normalizedDatabase);

        cancellationToken.ThrowIfCancellationRequested();
        await using (var stream = File.Create(fullPath))
        {
            await JsonSerializer.SerializeAsync(stream, payload, SerializerOptions, cancellationToken);
        }

        return new TableWriteResult(
            normalizedDatabase,
            payload.TableName,
            fullPath,
            overwrote);
    }

    public async Task<TableBatchWriteResult> ReplaceTablesAsync(
        string databaseName,
        IReadOnlyCollection<TableMetadataDocument> documents,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(documents);

        var normalizedDatabase = NormalizeDatabaseName(databaseName);
        var directory = EnsureTablesDirectory(normalizedDatabase);

        var writtenFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var fileNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var queue = new List<(TableMetadataDocument Payload, string FilePath)>(documents.Count);

        foreach (var document in documents)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (document is null)
                continue;

            ValidateDocument(document);
            var payload = NormalizeDocument(document, normalizedDatabase);
            var fileName = BuildUniqueFileName(payload.TableName, fileNames);
            var fullPath = Path.Combine(directory, fileName);
            queue.Add((payload, fullPath));
            writtenFiles.Add(fullPath);
        }

        foreach (var entry in queue)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using var stream = File.Create(entry.FilePath);
            await JsonSerializer.SerializeAsync(stream, entry.Payload, SerializerOptions, cancellationToken);
        }

        RemoveStaleFiles(directory, writtenFiles);

        return new TableBatchWriteResult(
            normalizedDatabase,
            queue.Count,
            queue.Select(item => item.FilePath).ToList());
    }

    private string EnsureTablesDirectory(string databaseName)
    {
        if (string.IsNullOrWhiteSpace(_options.MetadataDirectory))
            throw new InvalidOperationException("Metadata directory is not configured.");

        var path = Path.Combine(_options.MetadataDirectory, databaseName, "Tables");
        Directory.CreateDirectory(path);
        return path;
    }

    private static string NormalizeDatabaseName(string databaseName)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty.", nameof(databaseName));

        return databaseName.Trim();
    }

    private static void ValidateDocument(TableMetadataDocument document)
    {
        if (document is null)
            throw new ArgumentNullException(nameof(document));

        if (string.IsNullOrWhiteSpace(document.TableName))
            throw new ArgumentException("Table metadata document must define a table name.", nameof(document));

        if (string.IsNullOrWhiteSpace(SanitizeFileName(document.TableName)))
            throw new ArgumentException("Table name must contain at least one valid character.", nameof(document));

        if (document.Fields is null || document.Fields.Count == 0)
            throw new ArgumentException("At least one field is required.", nameof(document));

        foreach (var field in document.Fields)
        {
            if (field is null)
                throw new ArgumentException("Field definitions cannot be null.", nameof(document));

            if (string.IsNullOrWhiteSpace(field.Name))
                throw new ArgumentException("Field name cannot be empty.", nameof(document));

            if (string.IsNullOrWhiteSpace(field.DataType))
                throw new ArgumentException("Field data type cannot be empty.", nameof(document));
        }
    }

    private static TableMetadataDocument NormalizeDocument(TableMetadataDocument source, string databaseName)
    {
        var normalized = new TableMetadataDocument
        {
            DatabaseName = databaseName,
            TableName = source.TableName?.Trim() ?? string.Empty,
            Fields = new()
        };

        if (source.Fields is not null)
        {
            foreach (var field in source.Fields)
            {
                if (field is null)
                    continue;

                normalized.Fields.Add(new FieldMetadataDocument
                {
                    Name = field.Name?.Trim() ?? string.Empty,
                    DataType = field.DataType?.Trim() ?? string.Empty,
                    Length = Math.Max(0, field.Length),
                    AllowBlank = field.AllowBlank,
                    AutoGenerated = field.AutoGenerated
                });
            }
        }

        return normalized;
    }

    private static string BuildStableFileName(string tableName)
    {
        var sanitized = SanitizeFileName(tableName);
        return string.IsNullOrWhiteSpace(sanitized)
            ? $"table_{Guid.NewGuid():N}.json"
            : $"{sanitized}.json";
    }

    private static string BuildUniqueFileName(string tableName, HashSet<string> usedNames)
    {
        var sanitized = SanitizeFileName(tableName);
        if (string.IsNullOrWhiteSpace(sanitized))
            sanitized = $"table_{Guid.NewGuid():N}";

        var candidate = $"{sanitized}.json";
        var suffix = 1;

        while (usedNames.Contains(candidate))
        {
            candidate = $"{sanitized}_{suffix}.json";
            suffix++;
        }

        usedNames.Add(candidate);
        return candidate;
    }

    private static string SanitizeFileName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var trimmed = value.Trim();
        var chars = trimmed
            .Select(ch => InvalidFileNameCharacters.Contains(ch) || char.IsWhiteSpace(ch) ? '_' : ch)
            .ToArray();

        return new string(chars).Trim('_');
    }

    private void RemoveStaleFiles(string directory, HashSet<string> writtenFiles)
    {
        foreach (var file in Directory.EnumerateFiles(directory, "*.json", SearchOption.TopDirectoryOnly))
        {
            if (writtenFiles.Contains(file))
                continue;

            try
            {
                File.Delete(file);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                _logger.LogWarning(exception, "Failed to delete stale table metadata file {FilePath}", file);
            }
        }
    }
}
