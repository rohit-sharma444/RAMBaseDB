namespace PayrollRazorApp.Pages.Employees;

using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.Mvc.Rendering;
using PayrollRazorApp.Data;
using PayrollRazorApp.Data.Entities;
using System.Linq;

public class IndexModel : PageModel
{
    private readonly PayrollRepository _repository;

    public IndexModel(PayrollRepository repository)
    {
        _repository = repository;
    }

    public IReadOnlyList<Employee> Employees { get; private set; } = Array.Empty<Employee>();
    public IReadOnlyDictionary<int, string> CompanyLookup { get; private set; } = new Dictionary<int, string>();
    public IReadOnlyDictionary<int, string> DepartmentLookup { get; private set; } = new Dictionary<int, string>();
    public SelectList CompanyOptions { get; private set; } = default!;
    public SelectList DepartmentOptions { get; private set; } = default!;

    [BindProperty]
    public EmployeeInput Input { get; set; } = new();

    public void OnGet()
    {
        LoadData();
    }

    public IActionResult OnPost()
    {
        if (!ModelState.IsValid)
        {
            LoadData();
            return Page();
        }

        try
        {
            _repository.AddEmployee(new Employee
            {
                CompanyId = Input.CompanyId,
                DepartmentId = Input.DepartmentId,
                EmployeeNumber = Input.EmployeeNumber.Trim(),
                FirstName = Input.FirstName.Trim(),
                LastName = Input.LastName.Trim(),
                Email = Input.Email?.Trim(),
                Phone = Input.Phone?.Trim(),
                HireDate = Input.HireDate,
                Salary = Input.Salary
            });
        }
        catch (InvalidOperationException ex)
        {
            ModelState.AddModelError(string.Empty, ex.Message);
            LoadData();
            return Page();
        }

        return RedirectToPage();
    }

    private void LoadData()
    {
        var companies = _repository.GetCompanies();
        var departments = _repository.GetDepartments();
        CompanyLookup = companies.ToDictionary(company => company.Id, company => company.Name);
        DepartmentLookup = departments.ToDictionary(department => department.Id, department => department.Name);
        CompanyOptions = new SelectList(companies, nameof(Company.Id), nameof(Company.Name));
        DepartmentOptions = new SelectList(departments, nameof(Department.Id), nameof(Department.Name));
        Employees = _repository.GetEmployees();
    }

    public sealed class EmployeeInput
    {
        [Display(Name = "Company")]
        [Range(1, int.MaxValue, ErrorMessage = "Select a company")]
        public int CompanyId { get; set; }

        [Display(Name = "Department")]
        [Range(1, int.MaxValue, ErrorMessage = "Select a department")]
        public int DepartmentId { get; set; }

        [Required]
        [Display(Name = "Employee #")]
        public string EmployeeNumber { get; set; } = string.Empty;

        [Required]
        [Display(Name = "First name")]
        public string FirstName { get; set; } = string.Empty;

        [Required]
        [Display(Name = "Last name")]
        public string LastName { get; set; } = string.Empty;

        [EmailAddress]
        public string? Email { get; set; }

        [Phone]
        public string? Phone { get; set; }

        [DataType(DataType.Date)]
        [Display(Name = "Hire date")]
        public DateTime HireDate { get; set; } = DateTime.UtcNow.Date;

        [Display(Name = "Salary (USD)")]
        [Range(typeof(decimal), "0", "100000000", ErrorMessage = "Salary must be non-negative")]
        public decimal Salary { get; set; }
    }
}
