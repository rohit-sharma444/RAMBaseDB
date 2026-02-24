namespace PayrollRazorApp.Pages.Departments;

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

    public IReadOnlyList<Department> Departments { get; private set; } = Array.Empty<Department>();
    public IReadOnlyDictionary<int, string> CompanyLookup { get; private set; } = new Dictionary<int, string>();
    public SelectList CompanyOptions { get; private set; } = default!;

    [BindProperty]
    public DepartmentInput Input { get; set; } = new();

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
            _repository.AddDepartment(new Department
            {
                Name = Input.Name.Trim(),
                CompanyId = Input.CompanyId,
                Description = Input.Description?.Trim()
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
        CompanyLookup = companies.ToDictionary(company => company.Id, company => company.Name);
        CompanyOptions = new SelectList(companies, nameof(Company.Id), nameof(Company.Name));
        Departments = _repository.GetDepartments();
    }

    public sealed class DepartmentInput
    {
        [Required]
        [Display(Name = "Department name")]
        public string Name { get; set; } = string.Empty;

        [Display(Name = "Company")]
        [Range(1, int.MaxValue, ErrorMessage = "Select a company")]
        public int CompanyId { get; set; }

        [Display(Name = "Description")]
        public string? Description { get; set; }
    }
}
