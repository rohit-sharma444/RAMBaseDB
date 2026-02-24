namespace RAMBaseDB.Tests;

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Schema;
using Xunit;

public sealed class DatabaseBackupManagerTests
{
    [Fact]
    public async Task TriggerBackup_CreatesSnapshotAndPrunesHistory()
    {
        const string databaseName = "BackupDb";
        var backupDirectory = CreateTempDirectory();

        try
        {
            using var engine = CreateEngine(databaseName);
            SeedSampleData(engine, databaseName);

            using var manager = new DatabaseBackupManager(
                engine,
                databaseName,
                backupDirectory,
                backupInterval: TimeSpan.FromHours(1),
                maxBackupHistory: 2);

            await CreateBackupAsync(manager);
            var filesAfterFirstBackup = Directory.GetFiles(backupDirectory, "*.json.gz");
            Assert.Single(filesAfterFirstBackup);
            var firstBackupName = Path.GetFileName(filesAfterFirstBackup[0]);

            await CreateBackupAsync(manager);
            var filesAfterSecondBackup = Directory.GetFiles(backupDirectory, "*.json.gz").OrderBy(path => path).ToArray();
            Assert.Equal(2, filesAfterSecondBackup.Length);

            await CreateBackupAsync(manager);
            var filesAfterThirdBackup = Directory.GetFiles(backupDirectory, "*.json.gz").ToArray();
            Assert.Equal(2, filesAfterThirdBackup.Length);
            Assert.DoesNotContain(filesAfterThirdBackup, path => string.Equals(Path.GetFileName(path), firstBackupName, StringComparison.OrdinalIgnoreCase));
        }
        finally
        {
            DeleteDirectory(backupDirectory);
        }
    }

    [Fact]
    public void TryRestoreLatestBackup_HydratesNewestSnapshot()
    {
        const string databaseName = "RestoreDb";
        var backupDirectory = CreateTempDirectory();

        try
        {
            using (var engine = CreateEngine(databaseName))
            {
                var table = SeedSampleData(engine, databaseName);

                using var manager = new DatabaseBackupManager(
                    engine,
                    databaseName,
                    backupDirectory,
                    backupInterval: TimeSpan.FromHours(1));

                manager.TriggerBackup();

                table.Insert(new TestEntity { Name = "Beta", Email = "beta@example.com" });
                manager.TriggerBackup();
            }

            using var restoredEngine = CreateEngine(databaseName);
            using var restoreManager = new DatabaseBackupManager(
                restoredEngine,
                databaseName,
                backupDirectory,
                backupInterval: TimeSpan.FromHours(1));

            var restored = restoreManager.TryRestoreLatestBackup();
            Assert.True(restored);

            var restoredTable = restoredEngine.GetTable<TestEntity>(databaseName, "Users");
            var rows = restoredTable.AsQueryable().OrderBy(entity => entity.Id).ToArray();
            Assert.Equal(2, rows.Length);
            Assert.Equal(new[] { "Alpha", "Beta" }, rows.Select(entity => entity.Name));
        }
        finally
        {
            DeleteDirectory(backupDirectory);
        }
    }

    private static async Task CreateBackupAsync(DatabaseBackupManager manager)
    {
        manager.TriggerBackup();
        await Task.Delay(1100).ConfigureAwait(false);
    }

    private static DatabaseEngine CreateEngine(string databaseName)
        => new(new Database { Name = databaseName });

    private static Table<TestEntity> SeedSampleData(DatabaseEngine engine, string databaseName)
    {
        engine.CreateDatabase(databaseName);
        var table = engine.CreateTable<TestEntity>(databaseName, "Users");
        table.Insert(new TestEntity { Name = "Alpha", Email = "alpha@example.com" });
        return table;
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
