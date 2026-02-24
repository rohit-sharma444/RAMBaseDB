namespace RAMBaseDB.Domain.Abstractions;

public interface IUser
{
    bool HasPassword();
    bool IsPasswordsMatch();
    void Activate();
    void Deactivate();
    void RequirePasswordChange();
    void ClearPasswordChangeRequirement();
    void SetDefaultDatabase(string defaultDatabase);
    void ChangePassword(string password, string confirmPassword, bool markAsNeedingChange = false);

}
