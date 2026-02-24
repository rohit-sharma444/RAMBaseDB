namespace PayrollRazorApp.Data;

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PayrollRazorApp.Data.Entities;
using RAMBaseDB.Application;
using RAMBaseDB.Domain.Entities;
using System.IO;
using System.Linq;

/// <summary>
/// Small repository wrapper that bootstraps a RAMBaseDB database named "Payroll" and exposes typed tables for the
/// Razor UI to query and mutate.
/// </summary>
public class PayrollRepository : IDisposable
{
    private const string DatabaseName = "Payroll";
    private const int MaxBackupHistory = 12;
    private static readonly TimeSpan BackupInterval = TimeSpan.FromMinutes(5);
    private readonly DatabaseEngine _engine;
    private readonly Table<Company> _companies;
    private readonly Table<Department> _departments;
    private readonly Table<Employee> _employees;
    private readonly Table<EmployeeRecord> _employeeRecords;
    private readonly Table<AttendanceEntry> _attendanceEntries;
    private readonly ILogger<PayrollRepository>? _logger;
    private readonly DatabaseBackupManager _backupManager;

    public PayrollRepository(IHostEnvironment environment, ILogger<PayrollRepository>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(environment);

        _logger = logger;
        _engine = new DatabaseEngine(new Database { Name = DatabaseName });
        var backupDirectory = ResolveBackupDirectory(environment.ContentRootPath);
        _backupManager = new DatabaseBackupManager(
            _engine,
            DatabaseName,
            backupDirectory,
            BackupInterval,
            MaxBackupHistory,
            _logger);

        if (!_backupManager.TryRestoreLatestBackup() && !_engine.Exists(DatabaseName))
        {
            _engine.CreateDatabase(DatabaseName);
        }

        _companies = EnsureTable<Company>();
        _departments = EnsureTable<Department>();
        _employees = EnsureTable<Employee>();
        _employeeRecords = EnsureTable<EmployeeRecord>();
        _attendanceEntries = EnsureTable<AttendanceEntry>();

        Seed();
        _backupManager.TriggerBackup();
    }

    public IReadOnlyList<Company> GetCompanies()
        => _companies.AsQueryable().OrderBy(company => company.Name).ToList();

    public IReadOnlyList<Department> GetDepartments()
        => _departments.AsQueryable().OrderBy(department => department.Name).ToList();

    public IReadOnlyList<Employee> GetEmployees()
        => _employees.AsQueryable().OrderBy(employee => employee.LastName).ThenBy(employee => employee.FirstName).ToList();

    public IReadOnlyList<EmployeeRecord> GetEmployeeRecords()
        => _employeeRecords.AsQueryable().OrderByDescending(record => record.EffectiveDate).ThenBy(record => record.RecordType).ToList();

    public IReadOnlyList<AttendanceEntry> GetAttendanceEntries()
        => _attendanceEntries.AsQueryable().OrderByDescending(entry => entry.WorkDate).ThenBy(entry => entry.EmployeeId).ToList();

    public void AddCompany(Company company)
        => _companies.Insert(company);

    public void AddDepartment(Department department)
        => _departments.Insert(department);

    public void AddEmployee(Employee employee)
        => _employees.Insert(employee);

    public void AddEmployeeRecord(EmployeeRecord record)
        => _employeeRecords.Insert(record);

    public void AddAttendanceEntry(AttendanceEntry entry)
        => _attendanceEntries.Insert(entry);

    public bool CompanyExists(int companyId)
        => _companies.FindByPrimaryKey(companyId) is not null;

    public bool DepartmentExists(int departmentId)
        => _departments.FindByPrimaryKey(departmentId) is not null;

    public bool EmployeeExists(int employeeId)
        => _employees.FindByPrimaryKey(employeeId) is not null;

    public void Dispose()
    {
        _backupManager.Dispose();
        _engine.Dispose();
    }

    private Table<T> EnsureTable<T>() where T : class, new()
    {
        var tableName = typeof(T).Name;
        try
        {
            return _engine.GetTable<T>(DatabaseName, tableName);
        }
        catch (InvalidOperationException)
        {
            return _engine.CreateTable<T>(DatabaseName, tableName);
        }
    }

    private void Seed()
    {
        if (_companies.ToList().Count > 0)
        {
            return;
        }

        var contoso = new Company { Id = 1, Name = "Contoso Manufacturing", Address = "123 Industry Way", Description = "Global producer of industrial components." };
        var northwind = new Company { Id = 2, Name = "Northwind Traders", Address = "400 Market Street", Description = "Wholesale distributor of specialty foods." };
        _companies.InsertRange(new[] { contoso, northwind });

        var hr = new Department { Id = 1, CompanyId = contoso.Id, Name = "Human Resources", Description = "Core people operations team." };
        var it = new Department { Id = 2, CompanyId = contoso.Id, Name = "IT Operations", Description = "Infrastructure and automation." };
        var sales = new Department { Id = 3, CompanyId = northwind.Id, Name = "Sales", Description = "Field sales and account executives." };
        _departments.InsertRange(new[] { hr, it, sales });

        var employee1 = new Employee
        {
            Id = 1,
            CompanyId = contoso.Id,
            DepartmentId = hr.Id,
            EmployeeNumber = "CNT-1001",
            FirstName = "Ava",
            LastName = "Lopez",
            Email = "ava.lopez@contoso.com",
            Phone = "555-0100",
            HireDate = DateTime.UtcNow.Date.AddYears(-4),
            Salary = 78000m
        };
        var employee2 = new Employee
        {
            Id = 2,
            CompanyId = contoso.Id,
            DepartmentId = it.Id,
            EmployeeNumber = "CNT-1042",
            FirstName = "Jon",
            LastName = "Miller",
            Email = "jon.miller@contoso.com",
            Phone = "555-0111",
            HireDate = DateTime.UtcNow.Date.AddYears(-2),
            Salary = 92000m
        };
        var employee3 = new Employee
        {
            Id = 3,
            CompanyId = northwind.Id,
            DepartmentId = sales.Id,
            EmployeeNumber = "NWT-2004",
            FirstName = "Priya",
            LastName = "Singh",
            Email = "priya.singh@northwind.com",
            Phone = "555-0222",
            HireDate = DateTime.UtcNow.Date.AddYears(-1),
            Salary = 68000m
        };
        _employees.InsertRange(new[] { employee1, employee2, employee3 });

        var records = new[]
        {
            new EmployeeRecord { Id = 1, EmployeeId = employee1.Id, RecordType = "Salary", Details = "Annual merit increase", Amount = 3000m, EffectiveDate = DateTime.UtcNow.Date.AddMonths(-2) },
            new EmployeeRecord { Id = 2, EmployeeId = employee2.Id, RecordType = "Bonus", Details = "Automation initiative milestone", Amount = 4500m, EffectiveDate = DateTime.UtcNow.Date.AddMonths(-1) },
            new EmployeeRecord { Id = 3, EmployeeId = employee3.Id, RecordType = "Salary", Details = "New hire starting salary", Amount = 68000m, EffectiveDate = DateTime.UtcNow.Date.AddMonths(-6) }
        };
        _employeeRecords.InsertRange(records);

        var today = DateTime.UtcNow.Date;
        var attendance = new[]
        {
            new AttendanceEntry { Id = 1, EmployeeId = employee1.Id, WorkDate = today.AddDays(-1), Status = "Present", CheckIn = new TimeSpan(8, 55, 0), CheckOut = new TimeSpan(17, 10, 0), Notes = "Quarterly planning" },
            new AttendanceEntry { Id = 2, EmployeeId = employee2.Id, WorkDate = today.AddDays(-1), Status = "Remote", CheckIn = new TimeSpan(9, 5, 0), CheckOut = new TimeSpan(18, 0, 0), Notes = "Deployed payroll patch" },
            new AttendanceEntry { Id = 3, EmployeeId = employee3.Id, WorkDate = today.AddDays(-2), Status = "Leave", Notes = "Client site visit" }
        };
        _attendanceEntries.InsertRange(attendance);
    }

    private static string ResolveBackupDirectory(string contentRootPath)
    {
        var root = string.IsNullOrWhiteSpace(contentRootPath)
            ? Directory.GetCurrentDirectory()
            : contentRootPath;

        var directory = Path.Combine(root, "Snapshots", DatabaseName);
        Directory.CreateDirectory(directory);
        return directory;
    }

}
