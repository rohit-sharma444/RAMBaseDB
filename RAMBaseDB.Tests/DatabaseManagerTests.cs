namespace RAMBaseDB.Tests;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using RAMBaseDB.Infrastructure.Configuration;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Configuration;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Schema;
using Xunit;

public class DatabaseManagerTests
{
    [Fact]
    public void CreateDatabase_WithConfiguration_AllowsTableLifecycle()
    {
        using var manager = CreateManager("TenantDb");
        var tempDir = CreateTempDirectory();

        try
        {
            var configuration = CreateConfiguration("TenantDb", tempDir);

            manager.CreateDatabase(configuration);

            Assert.True(manager.Exists("TenantDb"));
            Assert.True(manager.TryGetConfiguration("TenantDb", out var registered));
            Assert.Same(configuration, registered);

            var table = manager.CreateTable<TestEntity>("TenantDb", "Users");
            table.Insert(new TestEntity { Name = "Alice", Email = "alice@example.com" });

            Assert.Equal(1, manager.TablesCount("TenantDb"));

            manager.ClearDatabase("TenantDb");
            Assert.Equal(0, manager.TablesCount("TenantDb"));
        }
        finally
        {
            DeleteDirectory(tempDir);
        }
    }

    [Fact]
    public void CreateDatabase_PersistsMetadataFile()
    {
        var tempWorkingDirectory = CreateTempDirectory();
        var originalWorkingDirectory = Directory.GetCurrentDirectory();
        var dumpDirectory = Path.Combine(tempWorkingDirectory, "dumps");

        try
        {
            Directory.SetCurrentDirectory(tempWorkingDirectory);

            using var manager = CreateManager("MetaDb");
            var configuration = CreateConfiguration("MetaDb", dumpDirectory);

            manager.CreateDatabase(configuration);

            var metadataFile = Path.Combine(tempWorkingDirectory, "Metadata", "MetaDb", "MetaDb-Metadata.json");
            Assert.True(File.Exists(metadataFile));

            var metadataContent = File.ReadAllText(metadataFile);
            Assert.False(string.IsNullOrWhiteSpace(metadataContent));
        }
        finally
        {
            Directory.SetCurrentDirectory(originalWorkingDirectory);
            DeleteDirectory(tempWorkingDirectory);
        }
    }

    [Fact]
    public void DumpAndLoadDatabase_RoundTripsTableData()
    {
        var tempDir = CreateTempDirectory();

        try
        {
            var dumpFile = Path.Combine(tempDir, "archive_dump.json.gz");

            using var manager = CreateManager("Archive");
            manager.CreateDatabase("Archive");
            var table = manager.CreateTable<TestEntity>("Archive", "Users");

            table.Insert(new TestEntity { Name = "Alice", Email = "alice@example.com" });
            table.Insert(new TestEntity { Name = "Bob", Email = "bob@example.com" });

            manager.DumpDatabase("Archive", dumpFile);
            Assert.True(File.Exists(dumpFile));

            using var restoredManager = CreateManager("Archive");
            restoredManager.LoadDatabase("Archive", dumpFile);

            var restoredTable = restoredManager.GetTable<TestEntity>("Archive", "Users");
            var rows = restoredTable.AsQueryable().OrderBy(row => row.Id).ToList();

            Assert.Equal(2, rows.Count);
            Assert.Equal(new[] { "Alice", "Bob" }, rows.Select(row => row.Name));
        }
        finally
        {
            DeleteDirectory(tempDir);
        }
    }

    [Fact]
    public void SerializeAndDeserializeDatabases_RoundTripsEntireCollection()
    {
        using var manager = CreateManager("Archive");
        manager.CreateDatabase("Archive");
        var users = manager.CreateTable<TestEntity>("Archive", "Users");
        users.Insert(new TestEntity { Name = "Alpha", Email = "alpha@example.com" });
        users.Insert(new TestEntity { Name = "Beta", Email = "beta@example.com" });

        manager.CreateDatabase("Logs");
        var logs = manager.CreateTable<TestEntity>("Logs", "Entries");
        logs.Insert(new TestEntity { Name = "Audit", Email = "audit@example.com" });

        var serialized = manager.SerializeDatabases();
        Assert.False(string.IsNullOrWhiteSpace(serialized));

        using var restored = CreateManager("Temp");
        restored.CreateDatabase("Temp");
        restored.CreateTable<TestEntity>("Temp", "Scratch").Insert(new TestEntity { Name = "Old", Email = "old@example.com" });

        restored.DeserializeDatabases(serialized);

        Assert.False(restored.Exists("Temp"));

        var restoredUsers = restored.GetTable<TestEntity>("Archive", "Users");
        var restoredEntries = restored.GetTable<TestEntity>("Logs", "Entries");

        var restoredNames = restoredUsers
            .AsQueryable()
            .OrderBy(row => row.Id)
            .Select(row => row.Name)
            .ToArray();

        Assert.Equal(new[] { "Alpha", "Beta" }, restoredNames);

        var entry = restoredEntries.AsQueryable().Single();
        Assert.Equal("Audit", entry.Name);
        Assert.Equal("audit@example.com", entry.Email);
    }

    [Fact]
    public async Task SerializeDatabases_AllowsConcurrentTableCreation()
    {
        const string databaseName = "Concurrent";
        using var manager = CreateManager(databaseName);
        manager.CreateDatabase(databaseName);
        var seed = manager.CreateTable<TestEntity>(databaseName, "Seed");
        seed.Insert(new TestEntity { Name = "Seed", Email = "seed@example.com" });

        const int iterations = 40;

        var serializationTask = Task.Run(() =>
        {
            for (var i = 0; i < iterations; i++)
            {
                var snapshot = manager.SerializeDatabases();
                Assert.False(string.IsNullOrWhiteSpace(snapshot));
            }
        });

        var tableCreationTask = Task.Run(() =>
        {
            for (var i = 0; i < iterations; i++)
            {
                var tableName = $"Users_{i:D3}";
                var table = manager.CreateTable<TestEntity>(databaseName, tableName);
                table.Insert(new TestEntity { Name = $"User {i}", Email = $"user{i}@example.com" });
            }
        });

        await Task.WhenAll(serializationTask, tableCreationTask);

        Assert.True(manager.TablesCount(databaseName) >= iterations + 1);
    }

    [Fact]
    public void DatabasesProperty_ReturnsSnapshotOfLoadedDatabases()
    {
        using var manager = CreateManager("Alpha");
        manager.CreateDatabase("Alpha");
        manager.CreateDatabase("Beta");

        var firstSnapshot = manager.Databases;
        Assert.Equal(2, firstSnapshot.Count);
        Assert.Contains(firstSnapshot, db => string.Equals(db.Name, "Alpha", StringComparison.Ordinal));
        Assert.Contains(firstSnapshot, db => string.Equals(db.Name, "Beta", StringComparison.Ordinal));

        var secondSnapshot = manager.Databases;
        Assert.False(ReferenceEquals(firstSnapshot, secondSnapshot));
        Assert.Equal(
            firstSnapshot.Select(db => db.Name),
            secondSnapshot.Select(db => db.Name));
    }

    [Fact]
    public void TrimSnapshotHistory_RemovesOldSnapshotsBeyondRetention()
    {
        using var manager = CreateManager("TenantSnapshots");
        var tempDir = CreateTempDirectory();

        try
        {
            const string prefix = "snapshot";
            var configuration = CreateConfiguration("TenantSnapshots", tempDir, maxSnapshotHistory: 2, filePrefix: prefix);

            var now = DateTime.UtcNow;
            for (var i = 0; i < 4; i++)
            {
                var filePath = Path.Combine(tempDir, $"{prefix}_{i:0000}.json.gz");
                File.WriteAllText(filePath, $"payload-{i}");
                File.SetLastWriteTimeUtc(filePath, now.AddMinutes(-i));
            }

            manager.TrimSnapshotHistory(configuration);

            var remaining = Directory
                .GetFiles(tempDir, $"{prefix}_*.json.gz")
                .OrderBy(path => path)
                .ToArray();

            Assert.Equal(2, remaining.Length);
            Assert.Contains(Path.Combine(tempDir, $"{prefix}_0000.json.gz"), remaining);
            Assert.Contains(Path.Combine(tempDir, $"{prefix}_0001.json.gz"), remaining);
        }
        finally
        {
            DeleteDirectory(tempDir);
        }
    }

    private static DatabaseEngine CreateManager(string databaseName)
        => new DatabaseEngine(new Database { Name = databaseName });

    private static DatabaseConfiguration CreateConfiguration(
        string databaseName,
        string dumpDirectory,
        int? maxSnapshotHistory = null,
        string? filePrefix = null)
    {
        var fullDumpDirectory = Path.GetFullPath(dumpDirectory);
        Directory.CreateDirectory(fullDumpDirectory);

        var configModel = new DatabaseConfigurationModel
        {
            DatabaseName = databaseName,
            DumpDirectory = fullDumpDirectory,
            DumpFilePrefix = filePrefix ?? "dump",
            EnableAutomaticSnapshots = true,
            SnapshotInterval = TimeSpan.FromSeconds(900),
            MaxSnapshotHistory = maxSnapshotHistory ?? 10,
            AutoRestoreLatestDump = false
        };

        return new DatabaseConfiguration(databaseName, fullDumpDirectory)
        {
            DataConfig = configModel
        };
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "RAMBaseDB", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteDirectory(string path)
    {
        if (string.IsNullOrWhiteSpace(path) || !Directory.Exists(path))
            return;

        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private class TestEntity
    {
        [PrimaryKey]
        [AutoIncrement]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; } = string.Empty;

        public string Email { get; set; } = string.Empty;
    }
}
