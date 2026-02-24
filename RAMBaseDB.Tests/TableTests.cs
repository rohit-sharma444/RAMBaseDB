namespace RAMBaseDB.Tests;

using System;
using System.Linq;
using System.Threading.Tasks;
using RAMBaseDB.Domain.Entities;
using RAMBaseDB.Domain.Schema;
using Xunit;

public class TableTests
{
    /// <summary>
    /// Inserts a row and verifies that the auto-incremented primary key is assigned
    /// and that the inserted row is a clone of the source object.
    /// </summary>
    [Fact]
    public void Insert_AssignsAutoIncrementAndClonesSource()
    {
        var table = new Table<TestEntity>();
        var source = new TestEntity { Name = "Alice", Email = "alice@example.com" };

        table.Insert(source);

        var inserted = table.AsQueryable().Single();

        Assert.Equal(1, inserted.Id);
        Assert.Equal("Alice", inserted.Name);
        Assert.NotSame(source, inserted);

        source.Name = "Updated";
        Assert.Equal("Alice", inserted.Name);
    }


    /// <summary>
    /// Inserts a row with a preset primary key and then inserts another row without a primary key,
    /// verifying that the auto-increment value has advanced past the preset key.
    /// </summary>
    [Fact]
    public void Insert_WithPresetPrimaryKey_AdvancesAutoIncrement()
    {
        var table = new Table<TestEntity>();

        table.Insert(new TestEntity { Id = 10, Name = "Custom", Email = "custom@example.com" });
        table.Insert(new TestEntity { Name = "Generated", Email = "generated@example.com" });

        var generated = table.AsQueryable().Single(e => e.Name == "Generated");
        Assert.Equal(11, generated.Id);
    }


    /// <summary>
    /// Attempts to insert a row missing a required field and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void Insert_RequiredFieldMissing_Throws()
    {
        var table = new Table<TestEntity>();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            table.Insert(new TestEntity { Name = string.Empty, Email = "missing@example.com" }));

        Assert.Contains("required", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Finds a row by its primary key, then deletes it and verifies it is removed.
    /// </summary>
    [Fact]
    public void FindAndDeleteByPrimaryKey_Works()
    {
        var table = new Table<TestEntity>();
        table.Insert(new TestEntity { Name = "First", Email = "first@example.com" });
        table.Insert(new TestEntity { Name = "Second", Email = "second@example.com" });

        var second = table.FindByPrimaryKey(2);
        Assert.NotNull(second);
        Assert.Equal("Second", second!.Name);

        var removed = table.DeleteByPrimaryKey(2);
        Assert.True(removed);
        Assert.Null(table.FindByPrimaryKey(2));
    }


    /// <summary>
    /// Updates rows matching a predicate and verifies the correct number of rows are modified.
    /// </summary>
    [Fact]
    public void Update_ModifiesMatchingRowsAndReturnsCount()
    {
        var table = new Table<TestEntity>();
        table.Insert(new TestEntity { Name = "Alpha", Email = "alpha@example.com" });
        table.Insert(new TestEntity { Name = "Beta", Email = "beta@example.com" });

        var updated = table.Update(e => e.Name.StartsWith("A"), e => e.Email = "updated@example.com");

        Assert.Equal(1, updated);
        var alpha = table.FindByPrimaryKey(1);
        Assert.Equal("updated@example.com", alpha!.Email);
    }


    /// <summary>
    /// Queries rows using a predicate and verifies that the returned rows are clones,
    /// so that mutations do not affect the stored data.
    /// </summary>
    [Fact]
    public void Where_ReturnsClonesSoMutationsDoNotLeak()
    {
        var table = new Table<TestEntity>();
        table.Insert(new TestEntity { Name = "Snapshot", Email = "snapshot@example.com" });

        var snapshot = table.Where(e => e.Name == "Snapshot").Single();
        snapshot.Name = "Changed";

        var stored = table.FindByPrimaryKey(snapshot.Id);
        Assert.Equal("Snapshot", stored!.Name);
    }


    /// <summary>
    /// Performs concurrent inserts from multiple tasks and verifies data integrity.
    /// </summary>
    [Fact]
    public async Task Insert_StressTest_WithConcurrentInserts()
    {
        var table = new Table<TestEntity>();
        const int writerCount = 8;
        const int insertsPerWriter = 500;

        var tasks = Enumerable.Range(0, writerCount)
            .Select(_ => Task.Run(() =>
            {
                for (var i = 0; i < insertsPerWriter; i++)
                {
                    table.Insert(new TestEntity
                    {
                        Name = $"User-{Guid.NewGuid()}",
                        Email = $"user{i}@example.com"
                    });
                }
            }))
            .ToArray();

        await Task.WhenAll(tasks);

        var rows = table.AsQueryable().ToList();
        Assert.Equal(writerCount * insertsPerWriter, rows.Count);

        var ids = rows.Select(r => r.Id).OrderBy(id => id).ToArray();
        Assert.Equal(Enumerable.Range(1, rows.Count), ids);
        Assert.Equal(rows.Count, rows.Select(r => r.Name).Distinct().Count());
    }

    /// <summary>
    /// Inserts a large number of rows using InsertRange and verifies data integrity.
    /// </summary>
    [Fact]
    public void InsertRange_BulkInsert()
    {
        var table = new Table<TestEntity>();
        const int recordCount = 10000;

        var payload = Enumerable.Range(0, recordCount)
            .Select(i => new TestEntity
            {
                Name = $"BulkUser-{i}",
                Email = $"bulk{i}@example.com"
            })
            .ToList();

        table.InsertRange(payload);

        var rows = table.AsQueryable().OrderBy(r => r.Id).ToList();

        Assert.Equal(recordCount, rows.Count);
        //Assert.Equal(Enumerable.Range(1, recordCount), rows.Select(r => r.Id));

        //payload[0].Name = "Changed-0";
        //var lastIndex = payload.Count - 1;
        //payload[lastIndex].Email = "changed@example.com";

        //var firstStored = table.FindByPrimaryKey(1);
        //var lastStored = table.FindByPrimaryKey(recordCount);

        //Assert.Equal("BulkUser-0", firstStored!.Name);
        //Assert.Equal($"bulk{recordCount - 1}@example.com", lastStored!.Email);
    }


    /// <summary>
    /// Inserts multiple rows with preset primary keys using InsertRange,
    /// then inserts another row without a primary key to verify that the
    /// auto-increment value has advanced correctly.
    /// </summary>
    [Fact]
    public void InsertRange_WithPresetPrimaryKeys_AdvancesAutoIncrement()
    {
        var table = new Table<TestEntity>();

        table.InsertRange(new[]
        {
            new TestEntity { Id = 5, Name = "Manual-5", Email = "m5@example.com" },
            new TestEntity { Id = 12, Name = "Manual-12", Email = "m12@example.com" }
        });

        table.Insert(new TestEntity { Name = "Next", Email = "next@example.com" });

        var next = table.AsQueryable().Single(e => e.Name == "Next");
        Assert.Equal(13, next.Id);
    }


    /// <summary>
    /// Inserts multiple rows using InsertRange, then queries and verifies the inserted data.
    /// </summary>
    [Fact]
    public void InsertRange_ThenGetData_ReturnsInsertedRows()
    {
        var table = new Table<TestEntity>();

        table.InsertRange(new[]
        {
            new TestEntity { Name = "Bulk-1", Email = "bulk1@example.com" },
            new TestEntity { Name = "Bulk-2", Email = "bulk2@example.com" },
            new TestEntity { Name = "Other", Email = "other@example.com" }
        });

        var bulkRows = table.Where(e => e.Name.StartsWith("Bulk-")).OrderBy(e => e.Id).ToList();

        Assert.Equal(2, bulkRows.Count);
        Assert.Equal(new[] { "Bulk-1", "Bulk-2" }, bulkRows.Select(r => r.Name));

        var stored = table.FindByPrimaryKey(bulkRows[0].Id);
        Assert.Equal("Bulk-1", stored!.Name);
    }


    /// <summary>
    /// Deletes rows matching a predicate and verifies the correct number of rows are removed.
    /// </summary>
    [Fact]
    public void Delete_RemovesRowsMatchingPredicateAndReturnsCount()
    {
        var table = new Table<TestEntity>();
        table.InsertRange(new[]
        {
            new TestEntity { Name = "Keep", Email = "keep@example.com" },
            new TestEntity { Name = "Drop-1", Email = "drop1@example.com" },
            new TestEntity { Name = "Drop-2", Email = "drop2@example.com" }
        });

        var removed = table.Delete(e => e.Email.StartsWith("drop", StringComparison.OrdinalIgnoreCase));

        Assert.Equal(2, removed);
        var remaining = table.AsQueryable().ToList();
        Assert.Single(remaining);
        Assert.Equal("Keep", remaining[0].Name);
    }


    /// <summary>
    /// Attempts to delete a row by primary key that does not exist and verifies that false is returned.
    /// </summary>
    [Fact]
    public void DeleteByPrimaryKey_WhenRowMissing_ReturnsFalse()
    {
        var table = new Table<TestEntity>();
        table.Insert(new TestEntity { Name = "Existing", Email = "existing@example.com" });

        var removed = table.DeleteByPrimaryKey(99);

        Assert.False(removed);
        Assert.NotNull(table.FindByPrimaryKey(1));
    }


    /// <summary>
    /// Enumerates the table to get a snapshot of rows, then modifies the table
    /// and verifies that the snapshot remains unchanged.
    /// </summary>
    [Fact]  
    public void Enumerator_ReturnsSnapshotAndIsolatedRows()
    {
        var table = new Table<TestEntity>();
        table.Insert(new TestEntity { Name = "Snapshot", Email = "snapshot@example.com" });

        var snapshot = table.ToList();

        table.Insert(new TestEntity { Name = "NewRow", Email = "new@example.com" });

        Assert.Single(snapshot);
        snapshot[0].Name = "Mutated";

        var stored = table.FindByPrimaryKey(1);
        Assert.Equal("Snapshot", stored!.Name);

        var liveRows = table.AsQueryable().OrderBy(e => e.Id).Select(e => e.Name).ToArray();
        Assert.Equal(new[] { "Snapshot", "NewRow" }, liveRows);
    }


    /// <summary>
    /// Attempts to update a row by clearing a required field and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void Update_WhenRequiredFieldCleared_Throws()
    {
        var table = new Table<TestEntity>();
        table.Insert(new TestEntity { Name = "Valid", Email = "valid@example.com" });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            table.Update(_ => true, e => e.Name = string.Empty));

        Assert.Contains("required", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Inserts a row with a duplicate primary key and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void Insert_DuplicatePrimaryKey_Throws()
    {
        var table = new Table<ManualKeyEntity>();
        table.Insert(new ManualKeyEntity { Code = "USR-1", Name = "First" });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            table.Insert(new ManualKeyEntity { Code = "USR-1", Name = "Duplicate" }));

        Assert.Contains("primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Inserts a row with a null primary key and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void Insert_NullPrimaryKey_Throws()
    {
        var table = new Table<ManualKeyEntity>();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            table.Insert(new ManualKeyEntity { Name = "Missing" }));

        Assert.Contains("primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Attempts to update a row to change its primary key to a value that already exists
    /// and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void Update_ChangingPrimaryKeyToExistingValue_Throws()
    {
        var table = new Table<ManualKeyEntity>();
        table.Insert(new ManualKeyEntity { Code = "A", Name = "Alpha" });
        table.Insert(new ManualKeyEntity { Code = "B", Name = "Beta" });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            table.Update(e => e.Code == "B", e => e.Code = "A"));

        Assert.Contains("primary key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Inserts a child row with a foreign key referencing a non-existent parent
    /// and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void Insert_ForeignKeyRequiresExistingParent()
    {
        var parents = new Table<ParentEntity>();
        var children = new Table<ChildEntity>();
        Assert.Empty(parents.AsQueryable());

        var ex = Assert.Throws<InvalidOperationException>(() =>
            children.Insert(new ChildEntity { ParentId = 42, Name = "Orphan" }));

        Assert.Contains("Foreign key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Inserts a child row with a foreign key referencing an existing parent
    /// and verifies that the insert succeeds.
    /// </summary>
    [Fact]
    public void Insert_ForeignKeySucceedsWhenParentExists()
    {
        var parents = new Table<ParentEntity>();
        parents.Insert(new ParentEntity { Name = "Parent" });
        var children = new Table<ChildEntity>();

        children.Insert(new ChildEntity { ParentId = 1, Name = "Child" });

        var stored = children.FindByPrimaryKey(1);
        Assert.NotNull(stored);
        Assert.Equal(1, stored!.ParentId);
    }


    /// <summary>
    /// Attempts to delete a parent row that is referenced by a child
    /// and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void DeleteParent_WhenChildExists_Throws()
    {
        var parents = new Table<ParentEntity>();
        parents.Insert(new ParentEntity { Name = "Parent" });
        var children = new Table<ChildEntity>();
        children.Insert(new ChildEntity { ParentId = 1, Name = "Child" });

        var ex = Assert.Throws<InvalidOperationException>(() => parents.DeleteByPrimaryKey(1));
        Assert.Contains("referenced", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Ensures predicate-based deletes enforce referential integrity just like DeleteByPrimaryKey.
    /// </summary>
    [Fact]
    public void DeleteParent_WithPredicateAndChildRows_Throws()
    {
        var parents = new Table<ParentEntity>();
        parents.Insert(new ParentEntity { Name = "Parent" });
        var children = new Table<ChildEntity>();
        children.Insert(new ChildEntity { ParentId = 1, Name = "Child" });

        var ex = Assert.Throws<InvalidOperationException>(() => parents.Delete(p => p.Id == 1));
        Assert.Contains("referenced", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Ensures the foreign key cache refreshes when a dependent table is registered after a previous delete primed the cache.
    /// </summary>
    [Fact]
    public void DeleteParent_WhenChildTableRegisteredLater_StillThrows()
    {
        var parents = new Table<ParentEntity>();
        parents.Insert(new ParentEntity { Name = "Parent-A" });
        parents.Insert(new ParentEntity { Name = "Parent-B" });

        // Prime the metadata cache before the dependent table exists.
        var removed = parents.DeleteByPrimaryKey(2);
        Assert.True(removed);

        var lateChildren = new Table<LateChildEntity>();
        lateChildren.Insert(new LateChildEntity { ParentId = 1, Name = "LateChild" });

        var ex = Assert.Throws<InvalidOperationException>(() => parents.DeleteByPrimaryKey(1));
        Assert.Contains("referenced", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Deletes child rows referencing a parent, then deletes the parent
    /// and verifies that the deletion succeeds.
    /// </summary>
    [Fact]
    public void DeleteParent_AfterRemovingChildren_Succeeds()
    {
        var parents = new Table<ParentEntity>();
        parents.Insert(new ParentEntity { Name = "Parent" });
        var children = new Table<ChildEntity>();
        children.Insert(new ChildEntity { ParentId = 1, Name = "Child" });

        children.Delete(c => c.ParentId == 1);
        var removed = parents.DeleteByPrimaryKey(1);

        Assert.True(removed);
    }


    /// <summary>
    /// Attempts to update a child row to set its foreign key to an invalid value
    /// and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void Update_ForeignKeyToInvalidValue_Throws()
    {
        var parents = new Table<ParentEntity>();
        parents.Insert(new ParentEntity { Name = "P1" });
        parents.Insert(new ParentEntity { Name = "P2" });
        var children = new Table<ChildEntity>();
        children.Insert(new ChildEntity { ParentId = 1, Name = "Child" });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            children.Update(c => c.ParentId == 1, c => c.ParentId = 999));

        Assert.Contains("Foreign key", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Attempts to update a parent's primary key while it is referenced by a child
    /// and verifies that an exception is thrown.
    /// </summary>
    [Fact]
    public void Update_PrimaryKeyWhileReferenced_Throws()
    {
        var parents = new Table<ParentEntity>();
        parents.Insert(new ParentEntity { Name = "Parent" });
        var children = new Table<ChildEntity>();
        children.Insert(new ChildEntity { ParentId = 1, Name = "Child" });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            parents.Update(p => p.Id == 1, p => p.Id = 2));

        Assert.Contains("referenced", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Verifies that the cached metadata also blocks primary key updates when a dependent table
    /// is registered after the cache was first populated.
    /// </summary>
    [Fact]
    public void Update_PrimaryKeyWithLateRegisteredChild_Throws()
    {
        var parents = new Table<ParentEntity>();
        parents.Insert(new ParentEntity { Name = "Parent-A" });
        parents.Insert(new ParentEntity { Name = "Parent-B" });

        // Prime cache via delete on second parent.
        var removed = parents.DeleteByPrimaryKey(2);
        Assert.True(removed);

        var lateChildren = new Table<LateChildEntity>();
        lateChildren.Insert(new LateChildEntity { ParentId = 1, Name = "LateChild" });

        var ex = Assert.Throws<InvalidOperationException>(() =>
            parents.Update(p => p.Id == 1, p => p.Id = 3));

        Assert.Contains("referenced", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ConcurrentReadersAndWriter_RemainThreadSafe()
    {
        var table = new Table<TestEntity>();
        table.Insert(new TestEntity { Name = "Seed", Email = "seed@example.com" });

        const int readerCount = 4;
        const int readerIterations = 200;
        const int totalInserts = readerCount * readerIterations;

        var readerTasks = Enumerable.Range(0, readerCount)
            .Select(_ => Task.Run(() =>
            {
                for (var i = 0; i < readerIterations; i++)
                {
                    var snapshot = table.ToList();
                    Assert.True(snapshot.All(entity => entity.Id > 0));
                }
            }))
            .ToArray();

        var writerTask = Task.Run(() =>
        {
            for (var i = 0; i < totalInserts; i++)
            {
                table.Insert(new TestEntity { Name = $"User {i}", Email = $"user{i}@example.com" });
            }
        });

        await Task.WhenAll(readerTasks.Append(writerTask));

        Assert.True(table.ToList().Count >= totalInserts + 1);
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

    private class ManualKeyEntity
    {
        [PrimaryKey]
        public string? Code { get; set; }

        [Required]
        public string Name { get; set; } = string.Empty;
    }

    private class ParentEntity
    {
        [PrimaryKey]
        [AutoIncrement]
        public int Id { get; set; }

        [Required]
        public string Name { get; set; } = string.Empty;
    }

    private class ChildEntity
    {
        [PrimaryKey]
        [AutoIncrement]
        public int Id { get; set; }

        [ForeignKey(typeof(ParentEntity))]
        [Required]
        public int ParentId { get; set; }

        [Required]
        public string Name { get; set; } = string.Empty;
    }

    private class LateChildEntity
    {
        [PrimaryKey]
        [AutoIncrement]
        public int Id { get; set; }

        [ForeignKey(typeof(ParentEntity))]
        [Required]
        public int ParentId { get; set; }

        [Required]
        public string Name { get; set; } = string.Empty;
    }
}
