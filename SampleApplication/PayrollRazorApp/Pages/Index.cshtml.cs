using Microsoft.AspNetCore.Mvc.RazorPages;
using PayrollRazorApp.Data;
using PayrollRazorApp.Data.Entities;
using System.Linq;

namespace PayrollRazorApp.Pages;

public class IndexModel : PageModel
{
    private readonly PayrollRepository _repository;

    public IndexModel(PayrollRepository repository)
    {
        _repository = repository;
    }

    public int CompanyCount { get; private set; }
    public int DepartmentCount { get; private set; }
    public int EmployeeCount { get; private set; }
    public int AttendanceCount { get; private set; }
    public int EmployeeRecordCount { get; private set; }
    public IReadOnlyList<Employee> FeaturedEmployees { get; private set; } = Array.Empty<Employee>();

    public void OnGet()
    {
        var companies = _repository.GetCompanies();
        var departments = _repository.GetDepartments();
        var employees = _repository.GetEmployees();

        CompanyCount = companies.Count;
        DepartmentCount = departments.Count;
        EmployeeCount = employees.Count;
        AttendanceCount = _repository.GetAttendanceEntries().Count;
        EmployeeRecordCount = _repository.GetEmployeeRecords().Count;
        FeaturedEmployees = employees.Take(3).ToList();
    }
}
