namespace PayrollRazorApp.Data.Entities;

using RAMBaseDB.Domain.Schema;

public class Department
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    [Required]
    public string Name { get; set; } = string.Empty;

    [ForeignKey(typeof(Company))]
    public int CompanyId { get; set; }

    public string? Description { get; set; }
}
