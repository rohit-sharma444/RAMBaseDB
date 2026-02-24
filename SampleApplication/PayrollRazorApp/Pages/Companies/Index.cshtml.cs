namespace PayrollRazorApp.Pages.Companies;

using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using PayrollRazorApp.Data;
using PayrollRazorApp.Data.Entities;

public class IndexModel : PageModel
{
    private readonly PayrollRepository _repository;

    public IndexModel(PayrollRepository repository)
    {
        _repository = repository;
    }

    public IReadOnlyList<Company> Companies { get; private set; } = Array.Empty<Company>();

    [BindProperty]
    public CompanyInput Input { get; set; } = new();

    public void OnGet()
    {
        LoadCompanies();
    }

    public IActionResult OnPost()
    {
        if (!ModelState.IsValid)
        {
            LoadCompanies();
            return Page();
        }

        try
        {
            _repository.AddCompany(new Company
            {
                Name = Input.Name.Trim(),
                Address = Input.Address?.Trim(),
                Description = Input.Description?.Trim()
            });
        }
        catch (InvalidOperationException ex)
        {
            ModelState.AddModelError(string.Empty, ex.Message);
            LoadCompanies();
            return Page();
        }

        return RedirectToPage();
    }

    private void LoadCompanies()
        => Companies = _repository.GetCompanies();

    public sealed class CompanyInput
    {
        [Required]
        [Display(Name = "Company name")]
        public string Name { get; set; } = string.Empty;

        [Display(Name = "Street address")]
        public string? Address { get; set; }

        [Display(Name = "Description")]
        public string? Description { get; set; }
    }
}
