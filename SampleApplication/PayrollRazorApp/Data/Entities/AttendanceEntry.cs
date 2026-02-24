namespace PayrollRazorApp.Data.Entities;

using RAMBaseDB.Domain.Schema;

public class AttendanceEntry
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    [ForeignKey(typeof(Employee))]
    public int EmployeeId { get; set; }

    public DateTime WorkDate { get; set; } = DateTime.UtcNow.Date;

    [Required]
    public string Status { get; set; } = "Present";

    public TimeSpan? CheckIn { get; set; }

    public TimeSpan? CheckOut { get; set; }

    public string? Notes { get; set; }
}
