namespace PayrollRazorApp.Pages.Attendance;

using System.ComponentModel.DataAnnotations;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Microsoft.AspNetCore.Mvc.Rendering;
using PayrollRazorApp.Data;
using PayrollRazorApp.Data.Entities;
using System.Linq;

public class IndexModel : PageModel
{
    private static readonly string[] Statuses = ["Present", "Remote", "Leave", "Absent"];
    private readonly PayrollRepository _repository;

    public IndexModel(PayrollRepository repository)
    {
        _repository = repository;
    }

    public IReadOnlyList<AttendanceEntry> Entries { get; private set; } = Array.Empty<AttendanceEntry>();
    public IReadOnlyDictionary<int, string> EmployeeLookup { get; private set; } = new Dictionary<int, string>();
    public SelectList EmployeeOptions { get; private set; } = default!;
    public IEnumerable<SelectListItem> StatusOptions { get; private set; } = Enumerable.Empty<SelectListItem>();

    [BindProperty]
    public AttendanceInput Input { get; set; } = new();

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
            _repository.AddAttendanceEntry(new AttendanceEntry
            {
                EmployeeId = Input.EmployeeId,
                WorkDate = Input.WorkDate,
                Status = Input.Status,
                CheckIn = Input.CheckIn,
                CheckOut = Input.CheckOut,
                Notes = Input.Notes?.Trim()
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
        StatusOptions = Statuses.Select(status => new SelectListItem(status, status));
        Entries = _repository.GetAttendanceEntries();
    }

    public sealed class AttendanceInput
    {
        [Display(Name = "Employee")]
        [Range(1, int.MaxValue, ErrorMessage = "Select an employee")]
        public int EmployeeId { get; set; }

        [DataType(DataType.Date)]
        [Display(Name = "Work date")]
        public DateTime WorkDate { get; set; } = DateTime.UtcNow.Date;

        [Required]
        public string Status { get; set; } = "Present";

        [DataType(DataType.Time)]
        [Display(Name = "Check-in")]
        public TimeSpan? CheckIn { get; set; }

        [DataType(DataType.Time)]
        [Display(Name = "Check-out")]
        public TimeSpan? CheckOut { get; set; }

        public string? Notes { get; set; }
    }
}
