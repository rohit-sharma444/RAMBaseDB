namespace RAMBaseDB.Domain.Entities;

/// <summary>
/// Represents a user account with authentication and profile information.
/// </summary>
/// <remarks>The User class encapsulates properties related to user identification, authentication, and account
/// status. It is typically used to manage user credentials, profile details, and access control within an application.
/// All properties are read-write, allowing for creation and modification of user accounts as needed.</remarks>
public class User
{
    public string Id { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public string ConfirmPassword { get; set; } = string.Empty;

    public string DefaultDatabase { get; set; } = string.Empty;

    public bool UserMustChangePassword { get; set; } = true;

    public bool IsActive { get; set; } = true;
}
