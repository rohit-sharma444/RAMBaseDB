namespace RAMBaseDB.Domain.Metadata;

using System;

/// <summary>
/// Represents metadata information about a database, including its name, creation and modification timestamps, owner,
/// and description.
/// </summary>
/// <remarks>This class provides basic details for identifying and describing a database instance. It can be used
/// to display database information in management tools or for auditing purposes. All properties are initialized with
/// default values; update them as needed to reflect the actual database state.</remarks>
public class DatabaseMetadata
{
    public string DatabaseName { get; set; } = string.Empty;
    public DateTime CreatedAt { get; } = DateTime.UtcNow;
    public DateTime LastModifiedAt { get; } = DateTime.UtcNow;
    public string Owner { get; set; } = string.Empty;
    public string Description { get; set; } = "Database Created";
}
