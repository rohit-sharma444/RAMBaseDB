namespace PayrollRazorApp.Data.Entities;

using RAMBaseDB.Domain.Schema;

public class Employee
{
    [PrimaryKey, AutoIncrement]
    public int Id { get; set; }

    [ForeignKey(typeof(Company))]
    public int CompanyId { get; set; }

    [ForeignKey(typeof(Department))]
    public int DepartmentId { get; set; }

    [Required]
    public string EmployeeNumber { get; set; } = string.Empty;

    [Required]
    public string FirstName { get; set; } = string.Empty;

    [Required]
    public string LastName { get; set; } = string.Empty;

    public string? Email { get; set; }

    public string? Phone { get; set; }

    public DateTime HireDate { get; set; } = DateTime.UtcNow.Date;

    public decimal Salary { get; set; }
}
