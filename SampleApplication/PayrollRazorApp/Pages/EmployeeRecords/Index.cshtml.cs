namespace PayrollRazorApp.Pages.EmployeeRecords;

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

    public IReadOnlyList<EmployeeRecord> Records { get; private set; } = Array.Empty<EmployeeRecord>();
    public IReadOnlyDictionary<int, string> EmployeeLookup { get; private set; } = new Dictionary<int, string>();
    public SelectList EmployeeOptions { get; private set; } = default!;

    [BindProperty]
    public RecordInput Input { get; set; } = new();

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
            _repository.AddEmployeeRecord(new EmployeeRecord
            {
                EmployeeId = Input.EmployeeId,
                RecordType = Input.RecordType.Trim(),
                Details = Input.Details?.Trim(),
                Amount = Input.Amount,
                EffectiveDate = Input.EffectiveDate
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
        var employees = _repository.GetEmployees();
        EmployeeLookup = employees.ToDictionary(employee => employee.Id, employee => $"{employee.FirstName} {employee.LastName}");
        EmployeeOptions = new SelectList(
            employees.Select(e => new { e.Id, FullName = $"{e.FirstName} {e.LastName}" }),
            "Id",
            "FullName");
        Records = _repository.GetEmployeeRecords();
    }

    public sealed class RecordInput
    {
        [Display(Name = "Employee")]
        [Range(1, int.MaxValue, ErrorMessage = "Select an employee")]
        public int EmployeeId { get; set; }

        [Required]
        [Display(Name = "Record type")]
        public string RecordType { get; set; } = string.Empty;

        public string? Details { get; set; }

        [Range(typeof(decimal), "0", "100000000", ErrorMessage = "Amount must be non-negative")]
        public decimal Amount { get; set; }

        [DataType(DataType.Date)]
        [Display(Name = "Effective date")]
        public DateTime EffectiveDate { get; set; } = DateTime.UtcNow.Date;
    }
}
