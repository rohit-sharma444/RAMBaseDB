namespace PayrollRazorApp.Data.Entities;

using RAMBaseDB.Domain.Schema;

public class EmployeeRecord
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    [ForeignKey(typeof(Employee))]
    public int EmployeeId { get; set; }

    [Required]
    public string RecordType { get; set; } = string.Empty;

    public string? Details { get; set; }

    public decimal Amount { get; set; }

    public DateTime EffectiveDate { get; set; } = DateTime.UtcNow.Date;
}
