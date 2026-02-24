namespace RAMBaseDB.Tests;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RAMBaseDB.Domain.Metadata;
using RAMBaseDB.Infrastructure.Configuration;
using RAMBaseDB.Infrastructure.Services;
using Xunit;

public sealed class TableMetadataStorageTests : IDisposable
{
    private readonly string _workspaceRoot;
    private readonly ILoggerFactory _loggerFactory;

    public TableMetadataStorageTests()
    {
        _workspaceRoot = Path.Combine(Path.GetTempPath(), "RAMBaseDB.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_workspaceRoot);
        _loggerFactory = LoggerFactory.Create(builder => { });
    }

    [Fact]
    public async Task SaveTableAsync_WritesFileAndDetectsOverwrite()
    {
        var service = CreateService();
        var document = CreateDocument("Orders");

        var first = await service.SaveTableAsync("SalesDb", document);
        Assert.False(first.Overwritten);
        Assert.True(File.Exists(first.FilePath));

        var second = await service.SaveTableAsync("SalesDb", document);
        Assert.True(second.Overwritten);
    }

    [Fact]
    public async Task ReplaceTablesAsync_WritesDocumentsAndRemovesStale()
    {
        var service = CreateService();
        var initial = CreateDocument("LegacyTable");
        await service.SaveTableAsync("Ops", initial);

        var inputs = new[]
        {
            CreateDocument("Departments"),
            CreateDocument("Employees", ("EmployeeId", "UNIQUEIDENTIFIER"), ("Name", "NVARCHAR"))
        };

        var result = await service.ReplaceTablesAsync("Ops", inputs);

        Assert.Equal(2, result.TablesWritten);

        var tablesDirectory = Path.Combine(_workspaceRoot, "Ops", "Tables");
        var files = Directory.EnumerateFiles(tablesDirectory, "*.json", SearchOption.TopDirectoryOnly).ToList();

        Assert.Equal(2, files.Count);
        Assert.All(files, file => Assert.DoesNotContain("LegacyTable", Path.GetFileNameWithoutExtension(file), StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task ReplaceTablesAsync_AppendsSuffixForDuplicateNames()
    {
        var service = CreateService();

        var duplicates = new[]
        {
            CreateDocument("Audit"),
            CreateDocument("Audit", ("RecordId", "INT"), ("Notes", "NVARCHAR"))
        };

        var result = await service.ReplaceTablesAsync("Ops", duplicates);

        Assert.Equal(2, result.TablesWritten);

        var tablesDirectory = Path.Combine(_workspaceRoot, "Ops", "Tables");
        var files = Directory
            .EnumerateFiles(tablesDirectory, "*.json", SearchOption.TopDirectoryOnly)
            .Select(Path.GetFileName)
            .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        Assert.Collection(files,
            name => Assert.Equal("Audit.json", name),
            name => Assert.Equal("Audit_1.json", name));
    }

    private FileSystemTableMetadataStorage CreateService()
    {
        var options = Options.Create(new WorkspaceOptions
        {
            MetadataDirectory = _workspaceRoot
        });

        return new FileSystemTableMetadataStorage(
            options,
            _loggerFactory.CreateLogger<FileSystemTableMetadataStorage>());
    }

    private static TableMetadataDocument CreateDocument(string tableName, params (string Name, string Type)[] fields)
    {
        var document = new TableMetadataDocument
        {
            TableName = tableName
        };

        if (fields.Length == 0)
        {
            document.Fields.Add(new FieldMetadataDocument
            {
                Name = "Id",
                DataType = "INT"
            });
        }
        else
        {
            foreach (var field in fields)
            {
                document.Fields.Add(new FieldMetadataDocument
                {
                    Name = field.Name,
                    DataType = field.Type
                });
            }
        }

        return document;
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_workspaceRoot))
            {
                Directory.Delete(_workspaceRoot, recursive: true);
            }
        }
        catch
        {
            // Swallow cleanup exceptions in test tear-down.
        }

        _loggerFactory.Dispose();
    }
}
