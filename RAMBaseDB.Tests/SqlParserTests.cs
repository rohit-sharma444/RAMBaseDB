namespace RAMBaseDB.Tests;

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Abstractions;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Schema;
using Xunit;

public class SqlParserTests
{
    private const string DatabaseName = "TenantDb";

    [Fact]
    public void ExecuteSelect_WithJoinGroupingAndOrdering_ReturnsAggregatedRows()
    {
        using var manager = CreateManager();
        var parser = CreateParserWithTables(manager, out var customers, out var orders);

        customers.Insert(new Customer { Name = "Alice", Region = "North" });
        customers.Insert(new Customer { Name = "Bob", Region = "West" });

        orders.Insert(new Order { CustomerId = 1, Status = "Open", Total = 100m });
        orders.Insert(new Order { CustomerId = 1, Status = "Open", Total = 50m });
        orders.Insert(new Order { CustomerId = 2, Status = "Open", Total = 75m });
        orders.Insert(new Order { CustomerId = 2, Status = "Closed", Total = 300m });

        var sql = """
            SELECT c.Name AS CustomerName, o.Id AS OrderId, o.Total AS Total
            FROM Customers c
            JOIN Orders o ON c.Id = o.CustomerId
            WHERE o.Status = 'Open'
            ORDER BY Total DESC
            """;

        var result = parser.Execute(sql);

        Assert.True(result.IsQuery);
        Assert.NotNull(result.Rows);
        Assert.Equal(3, result.Rows!.Count);
        Assert.Equal(3, result.AffectedRows);

        dynamic first = result.Rows[0];
        dynamic second = result.Rows[1];
        dynamic third = result.Rows[2];

        Assert.Equal("Alice", first.CustomerName);
        Assert.Equal(100m, (decimal)first.Total);

        Assert.Equal("Bob", second.CustomerName);
        Assert.Equal(75m, (decimal)second.Total);

        Assert.Equal("Alice", third.CustomerName);
        Assert.Equal(50m, (decimal)third.Total);
    }

    [Fact]
    public void ExecuteInsert_AddsRowAndReportsAffectedRows()
    {
        using var manager = CreateManager();
        var parser = CreateParserWithTables(manager, out var customers, out var orders);
        _ = orders;

        var result = parser.Execute("INSERT INTO Customers (Name, Region) VALUES ('Clara', 'West')");

        Assert.False(result.IsQuery);
        Assert.Equal(1, result.AffectedRows);
        Assert.Null(result.Rows);

        var stored = customers.AsQueryable().Single();
        Assert.Equal("Clara", stored.Name);
        Assert.Equal("West", stored.Region);
        Assert.True(stored.Id > 0);
    }

    [Fact]
    public void ExecuteUpdate_ChangesMatchingRows()
    {
        using var manager = CreateManager();
        var parser = CreateParserWithTables(manager, out var customers, out var orders);

        customers.Insert(new Customer { Name = "Parent", Region = "North" });

        orders.Insert(new Order { CustomerId = 1, Status = "Pending", Total = 25m });
        orders.Insert(new Order { CustomerId = 1, Status = "Pending", Total = 35m });
        orders.Insert(new Order { CustomerId = 1, Status = "Closed", Total = 40m });

        var result = parser.Execute("UPDATE Orders SET Status = 'Closed', Total = 50.5 WHERE Status = 'Pending'");

        Assert.False(result.IsQuery);
        Assert.Equal(2, result.AffectedRows);

        var statuses = orders.AsQueryable().OrderBy(row => row.Id).Select(row => row.Status).ToArray();
        Assert.Equal(new[] { "Closed", "Closed", "Closed" }, statuses);

        var totals = orders.AsQueryable().OrderBy(row => row.Id).Select(row => row.Total).ToArray();
        Assert.Equal(new[] { 50.5m, 50.5m, 40m }, totals);
    }

    [Fact]
    public void ExecuteDelete_RemovesRowsMatchingPredicate()
    {
        using var manager = CreateManager();
        var parser = CreateParserWithTables(manager, out var customers, out var orders);

        customers.Insert(new Customer { Name = "Alpha", Region = "East" });

        orders.Insert(new Order { CustomerId = 1, Status = "Closed", Total = 10m });
        orders.Insert(new Order { CustomerId = 1, Status = "Closed", Total = 20m });
        orders.Insert(new Order { CustomerId = 1, Status = "Open", Total = 30m });

        var result = parser.Execute("DELETE FROM Orders WHERE Status = 'Closed'");

        Assert.False(result.IsQuery);
        Assert.Equal(2, result.AffectedRows);

        var remaining = orders.AsQueryable().ToList();
        Assert.Single(remaining);
        Assert.Equal("Open", remaining[0].Status);
        Assert.Equal(30m, remaining[0].Total);
    }

    private static SqlParser CreateParserWithTables(
        DatabaseEngine manager,
        out Table<Customer> customers,
        out Table<Order> orders)
    {
        manager.CreateDatabase(DatabaseName);
        customers = manager.CreateTable<Customer>(DatabaseName, "Customers");
        orders = manager.CreateTable<Order>(DatabaseName, "Orders");
        return new SqlParser(manager, DatabaseName);
    }

    private static DatabaseEngine CreateManager()
        => new DatabaseEngine(new Database { Name = DatabaseName });

    private class Customer
    {
        [PrimaryKey]
        [AutoIncrement]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; } = string.Empty;

        public string Region { get; set; } = string.Empty;
    }

    private class Order
    {
        [PrimaryKey]
        [AutoIncrement]
        public int Id { get; set; }

        [ForeignKey(typeof(Customer))]
        [Required]
        public int CustomerId { get; set; }

        [Required]
        public string Status { get; set; } = string.Empty;

        public decimal Total { get; set; }
    }

    private sealed class MutableTable<TRow> : ITable where TRow : class
    {
        private readonly List<TRow> _rows = new();

        public MutableTable(string name)
        {
            Name = name;
        }

        public string Name { get; set; }

        public Type DataType => typeof(TRow);

        public IReadOnlyList<TRow> Rows => _rows;

        public void Clear() => _rows.Clear();

        public void Add(object item)
        {
            if (item is not TRow typed)
                throw new InvalidOperationException($"Row type '{item?.GetType().Name ?? "null"}' does not match '{typeof(TRow).Name}'.");

            _rows.Add(typed);
        }

        public int Delete(Func<object, bool>? predicate)
        {
            if (predicate is null)
            {
                var removed = _rows.Count;
                _rows.Clear();
                return removed;
            }

            return _rows.RemoveAll(row => predicate(row!));
        }

        public int Update(Func<object, bool>? predicate, Action<object> mutator)
        {
            ArgumentNullException.ThrowIfNull(mutator);

            var matcher = predicate ?? (_ => true);
            var affected = 0;

            foreach (var row in _rows)
            {
                if (matcher(row!))
                {
                    mutator(row!);
                    affected++;
                }
            }

            return affected;
        }

        public IEnumerable GetRows() => _rows;
    }
}
