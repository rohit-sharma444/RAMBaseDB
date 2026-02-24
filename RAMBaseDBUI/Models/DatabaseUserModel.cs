using System.ComponentModel.DataAnnotations;

namespace RAMBaseDBUI.Models;

public class DatabaseUserModel
{
    [Required, StringLength(60)]
    [Display(Name = "First Name")]
    public string FirstName { get; set; } = string.Empty;

    [Required, StringLength(60)]
    [Display(Name = "Last Name")]
    public string LastName { get; set; } = string.Empty;

    [Required, EmailAddress]
    public string Email { get; set; } = string.Empty;

    [Required, StringLength(32, MinimumLength = 3)]
    public string Username { get; set; } = string.Empty;

    [Required]
    public string Role { get; set; } = "DB Admin";

    [Required]
    [Display(Name = "Access Scope")]
    public string AccessScope { get; set; } = "Read / Write";

    [Required, StringLength(64, MinimumLength = 8)]
    [Display(Name = "Temporary Password")]
    public string TemporaryPassword { get; set; } = "ChangeMe!123";

    [Display(Name = "Require Reset On First Login")]
    public bool ForcePasswordReset { get; set; } = true;

    [StringLength(80)]
    [Display(Name = "Team / Project Tag")]
    public string TeamTag { get; set; } = string.Empty;
}
