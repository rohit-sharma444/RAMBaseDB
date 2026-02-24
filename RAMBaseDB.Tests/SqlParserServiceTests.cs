namespace RAMBaseDB.Tests;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Schema;
using RAMBaseDB.Infrastructure.Services;
using Xunit;

public class SqlParserServiceTests
{
    private const string DatabaseName = "ServiceDb";

    [Fact]
    public async Task EnqueueAsync_ProcessesCommandsViaBackgroundWorker()
    {
        var manager = CreateManager();
        var service = new SqlParserService(manager, databaseName: DatabaseName);

        var orders = manager.CreateTable<ServiceOrder>(DatabaseName, "Orders");

        try
        {
            await service.StartAsync(CancellationToken.None);

            var insertResult = await service.EnqueueAsync("INSERT INTO Orders (Customer, Total) VALUES ('Alice', 42.5)");

            Assert.False(insertResult.IsQuery);
            Assert.Equal(1, insertResult.AffectedRows);

            var stored = orders.AsQueryable().Single();
            Assert.Equal("Alice", stored.Customer);
            Assert.Equal(42.5m, stored.Total);

            var queryResult = await service.EnqueueAsync("SELECT Customer, Total FROM Orders");

            Assert.True(queryResult.IsQuery);
            Assert.NotNull(queryResult.Rows);
            Assert.Single(queryResult.Rows!);

            dynamic row = queryResult.Rows![0];
            Assert.Equal("Alice", (string)row.Customer);
            Assert.Equal(42.5m, (decimal)row.Total);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            service.Dispose();
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithCanceledToken_CompletesTaskAsCanceled()
    {
        var manager = CreateManager();
        var service = new SqlParserService(manager, databaseName: DatabaseName);
        _ = manager.CreateTable<ServiceOrder>(DatabaseName, "Orders");

        using var cts = new CancellationTokenSource();
        var pending = service.EnqueueAsync("SELECT Id FROM Orders", cts.Token);
        cts.Cancel();

        try
        {
            await service.StartAsync(CancellationToken.None);

            await Assert.ThrowsAnyAsync<TaskCanceledException>(() => pending);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
            service.Dispose();
        }
    }

    [Fact]
    public async Task EnqueueAsync_WithEmptySql_ThrowsArgumentException()
    {
        var manager = CreateManager();
        var service = new SqlParserService(manager, databaseName: DatabaseName);

        try
        {
            var exception = await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await service.EnqueueAsync("  ");
            });

            Assert.Contains("SQL statement cannot be empty", exception.Message);
        }
        finally
        {
            service.Dispose();
        }
    }

    private class ServiceOrder
    {
        [PrimaryKey]
        [AutoIncrement]
        public int Id { get; set; }

        [Required]
        public string Customer { get; set; } = string.Empty;

        public decimal Total { get; set; }
    }

    private static DatabaseEngine CreateManager()
        => new DatabaseEngine(new Database { Name = DatabaseName });
}
