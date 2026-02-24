namespace RAMBaseDB.Application;

using RAMBaseDB.Domain.Abstractions;
using RAMBaseDB.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.Json;


/// <summary>
/// Provides functionality for creating, managing, validating, and persisting user accounts, including password
/// management and activation state. Supports serialization to and from persistent storage and dictionary
/// representations.
/// </summary>
/// <remarks>UserManager encapsulates user-related operations such as password changes, validation, and
/// activation/deactivation. It ensures that user data is consistent and valid before persistence or transport. The
/// class supports cloning, deep copying, and hydration from various sources, and is designed to prevent external
/// mutations from affecting internal state. Thread safety is not guaranteed; concurrent access should be managed
/// externally if required.</remarks>
public class UserManager : IUser, IEquatable<User>, IDisposable
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    private User _user = new();

    public UserManager(
        string id,
        string name,
        string password,
        string confirmPassword,
        string defaultDatabase,
        bool userMustChangePassword = false,
        bool isActive = true)
    {
        _user.Id = Normalize(id);
        _user.Name = Normalize(name);
        _user.Password = Normalize(password);
        _user.ConfirmPassword = Normalize(confirmPassword);
        _user.UserMustChangePassword = userMustChangePassword;
        if (isActive)
            Activate();
        else
            Deactivate();

        SetDefaultDatabase(defaultDatabase);
    }

    /// <summary>
    /// Creates and returns the internal UserDTO after ensuring validity.
    /// </summary>
    /// <returns></returns>
    public User Create()
    {
        EnsurePasswordConsistency();
        EnsureValid();
        return _user;
    }


    /// <summary>
    /// Password has been set (non-empty).
    /// </summary>
    public bool HasPassword() => _user.Password.Length > 0;

    /// <summary>
    /// Confirm Password has been set (non-empty).
    /// </summary>
    public bool HasConfirmPassword() => _user.ConfirmPassword.Length > 0;

    /// <summary>
    /// Password and confirmation match.
    /// </summary>
    public bool IsPasswordsMatch() => string.Equals(_user.Password, _user.ConfirmPassword, StringComparison.Ordinal);

    /// <summary>
    /// Activates the _user.
    /// </summary>
    public void Activate() => _user.IsActive = true;

    /// <summary>
    /// Deactivates the _user.
    /// </summary>
    public void Deactivate() => _user.IsActive = false;

    /// <summary>
    /// Requires the _user to change their password on next login.
    /// </summary>
    public void RequirePasswordChange() => _user.UserMustChangePassword = true;

    /// <summary>
    /// Clears the requirement for the _user to change their password.
    /// </summary>
    public void ClearPasswordChangeRequirement() => _user.UserMustChangePassword = false;

    public void Rename(string name)
    {
        _user.Name = Normalize(name);
        if (string.IsNullOrEmpty(_user.Name))
            throw new ArgumentException("_user name cannot be empty.", nameof(name));
    }

    /// <summary>
    /// Sets the default database for the _user.
    /// </summary>
    /// <param name="defaultDatabase"></param>
    /// <exception cref="ArgumentException"></exception>
    public void SetDefaultDatabase(string defaultDatabase)
    {
        _user.DefaultDatabase = Normalize(defaultDatabase);
        if (string.IsNullOrEmpty(_user.DefaultDatabase))
            throw new ArgumentException("Default database cannot be empty.", nameof(defaultDatabase));
    }

    /// <summary>
    /// Changes the _user's password, ensuring consistency and optionally marking
    /// </summary>
    /// <param name="password"></param>
    /// <param name="confirmPassword"></param>
    /// <param name="markAsNeedingChange"></param>
    public void ChangePassword(string password, string confirmPassword, bool markAsNeedingChange = false)
    {
        _user.Password = Normalize(password);
        _user.ConfirmPassword = Normalize(confirmPassword);
        _user.UserMustChangePassword = markAsNeedingChange;
        EnsurePasswordConsistency();
    }

    /// <summary>
    /// Performs a lightweight validation pass and returns all discovered issues.
    /// </summary>
    public bool TryValidate(out IReadOnlyList<string> errors)
    {
        var issues = new List<string>();

        if (string.IsNullOrWhiteSpace(_user.Id))
            issues.Add("Id is required.");
        if (string.IsNullOrWhiteSpace(_user.Name))
            issues.Add("Name is required.");
        if (string.IsNullOrWhiteSpace(_user.DefaultDatabase))
            issues.Add("Default database is required.");
        if (!HasPassword())
            issues.Add("Password is required.");
        if (!HasConfirmPassword())
            issues.Add("Password is required.");
        if (!IsPasswordsMatch())
            issues.Add("Password and confirmation must match.");

        errors = issues;
        return issues.Count == 0;
    }

    /// <summary>
    /// Throws when the DTO is not in a valid state.
    /// </summary>
    public void EnsureValid()
    {
        if (!TryValidate(out var errors))
            throw new InvalidOperationException($"UserDTO is invalid: {string.Join("; ", errors)}");
    }

    /// <summary>
    /// Friendly string representation for logging/debugging.
    /// </summary>
    public override string ToString()
        => $"{_user.Name} ({_user.Id}) - {(_user.IsActive ? "Active" : "Inactive")}";

    /// <summary>
    /// Serialises the DTO to a plain dictionary for persistence or transport layers.
    /// </summary>
    public Dictionary<string, object> ToDictionary()
    {
        return new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase)
        {
            ["Id"] = _user.Id,
            ["Name"] = _user.Name,
            ["Password"] = _user.Password,
            ["ConfirmPassword"] = _user.ConfirmPassword,
            ["DefaultDatabase"] = _user.DefaultDatabase,
            ["UserMustChangePassword"] = _user.UserMustChangePassword,
            ["IsActive"] = _user.IsActive
        };
    }

    /// <summary>
    /// Persists the current user to disk as JSON after validating the payload.
    /// </summary>
    public void SaveUser(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        EnsureValid();

        var directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrWhiteSpace(directory))
            Directory.CreateDirectory(directory);

        var payload = JsonSerializer.Serialize(_user, SerializerOptions);
        File.WriteAllText(filePath, payload);
    }

    /// <summary>
    /// Reads user data from the supplied file, validates it and returns a hydrated DTO.
    /// </summary>
    public static User LoadUser(string filePath)
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));
        if (!File.Exists(filePath))
            throw new FileNotFoundException($"User data file not found: {filePath}", filePath);

        var payload = File.ReadAllText(filePath);
        var dto = JsonSerializer.Deserialize<User>(payload, SerializerOptions)
            ?? throw new InvalidOperationException("Failed to deserialize user data.");

        var manager = new UserManager(
            dto.Id ?? string.Empty,
            dto.Name ?? string.Empty,
            dto.Password ?? string.Empty,
            string.IsNullOrWhiteSpace(dto.ConfirmPassword) ? dto.Password ?? string.Empty : dto.ConfirmPassword,
            dto.DefaultDatabase ?? string.Empty,
            dto.UserMustChangePassword,
            dto.IsActive);

        return manager.Create();
    }

    /// <summary>
    /// Hydrates a DTO from a dictionary representation, tolerating case differences and
    /// missing confirmation values (falls back to the password).
    /// </summary>
    public IUser FromDictionary(IReadOnlyDictionary<string, object?> values)
    {
        if (values is null)
            throw new ArgumentNullException(nameof(values));

        var id = ReadRequiredString(values, nameof(_user.Id));
        var name = ReadRequiredString(values, nameof(_user.Name));
        var password = ReadRequiredString(values, nameof(_user.Password));
        var confirm = TryReadString(values, nameof(_user.ConfirmPassword), out var confirmValue)
            ? confirmValue
            : password;
        var defaultDb = ReadRequiredString(values, nameof(_user.DefaultDatabase));
        var mustChange = ReadBool(values, nameof(_user.UserMustChangePassword), defaultValue: false);
        var isActive = ReadBool(values, nameof(_user.IsActive), defaultValue: true);

        return new UserManager(id, name, password, confirm, defaultDb, mustChange, isActive);
    }

    /// <summary>
    /// Creates a new <see cref="UserManager"/> from an existing <see cref="IUser"/> instance.
    /// Ensures the new instance owns its own state and isn't affected by external mutations.
    /// </summary>
    public UserManager FromUser(User _user)
    {
        ArgumentNullException.ThrowIfNull(_user);
        return new UserManager(
            _user.Id,
            _user.Name,
            _user.Password,
            _user.ConfirmPassword,
            _user.DefaultDatabase,
            _user.UserMustChangePassword,
            _user.IsActive);
    }

    /// <summary>
    /// Produces a deep copy and optionally applies additional mutations through <paramref name="mutate"/>.
    /// </summary>
    public IUser Clone(Action<IUser>? mutate = null)
    {
        var clone = new UserManager(_user.Id, _user.Name, _user.Password, _user.ConfirmPassword, _user.DefaultDatabase, _user.UserMustChangePassword, _user.IsActive);
        mutate?.Invoke(clone);
        clone.EnsureValid();
        return clone;
    }

    public override bool Equals(object? obj)
        => obj is IUser other && Equals(other);

    public bool Equals(User? other)
    {
        if (ReferenceEquals(this, other))
            return true;
        if (other is null)
            return false;

        return string.Equals(_user.Id, other.Id, StringComparison.OrdinalIgnoreCase);
    }

    public override int GetHashCode()
        => StringComparer.OrdinalIgnoreCase.GetHashCode(_user.Id);

    private void EnsurePasswordConsistency()
    {
        if (_user.Password.Length == 0 && _user.ConfirmPassword.Length == 0)
            return;

        if (!IsPasswordsMatch())
            throw new ArgumentException("Password and confirmation must match.", nameof(_user.ConfirmPassword));
    }

    private string Normalize(string? value)
        => value?.Trim() ?? string.Empty;

    private string ReadRequiredString(IReadOnlyDictionary<string, object?> values, string key)
    {
        if (TryReadString(values, key, out var value) && !string.IsNullOrEmpty(value))
            return value;

        throw new KeyNotFoundException($"Key '{key}' was not found in the supplied values.");
    }

    private bool TryReadString(IReadOnlyDictionary<string, object?> values, string key, out string value)
    {
        if (!TryGetValueCaseInsensitive(values, key, out var raw))
        {
            value = string.Empty;
            return false;
        }

        value = Normalize(raw switch
        {
            null => string.Empty,
            string s => s,
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => raw.ToString() ?? string.Empty
        });
        return true;
    }

    private bool ReadBool(IReadOnlyDictionary<string, object?> values, string key, bool defaultValue)
    {
        if (!TryGetValueCaseInsensitive(values, key, out var raw) || raw is null)
            return defaultValue;

        switch (raw)
        {
            case bool b:
                return b;

            case string s:
                var trimmed = s.Trim();
                if (bool.TryParse(trimmed, out var parsed))
                    return parsed;

                if (int.TryParse(trimmed, NumberStyles.Integer, CultureInfo.InvariantCulture, out var numeric))
                    return numeric != 0;

                if (string.Equals(trimmed, "yes", StringComparison.OrdinalIgnoreCase))
                    return true;
                if (string.Equals(trimmed, "no", StringComparison.OrdinalIgnoreCase))
                    return false;

                return defaultValue;

            case IConvertible convertible:
                try
                {
                    return convertible.ToBoolean(CultureInfo.InvariantCulture);
                }
                catch (FormatException)
                {
                    return defaultValue;
                }
                catch (InvalidCastException)
                {
                    return defaultValue;
                }
        }

        return defaultValue;
    }

    private bool TryGetValueCaseInsensitive(IReadOnlyDictionary<string, object?> values, string key, out object? value)
    {
        foreach (var pair in values)
        {
            if (string.Equals(pair.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                value = pair.Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    public void Dispose()
    {
    }
}
