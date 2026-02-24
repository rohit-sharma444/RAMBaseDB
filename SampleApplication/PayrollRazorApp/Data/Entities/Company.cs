namespace PayrollRazorApp.Data.Entities;

using RAMBaseDB.Domain.Schema;

public class Company
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    [Required]
    public string Name { get; set; } = string.Empty;

    public string? Address { get; set; }

    public string? Description { get; set; }
}
