namespace RAMBaseDB.Domain.Schema;

using System;
using System.Reflection;

/// <summary>
/// Represents metadata about a property mapped to a database column, including its name and key attributes.
/// </summary>
/// <remarks>Use this class to access information about how a property is represented in the database schema, such
/// as whether it is a primary key, required, or auto-incremented. This is typically used in scenarios involving
/// object-relational mapping or dynamic schema inspection.</remarks>
public class ColumnInfo
{
    public PropertyInfo Property { get; }
    public string Name => Property.Name;
    public bool IsPrimaryKey { get; }
    public bool IsRequired { get; }
    public bool IsAutoIncrement { get; }
    public bool IsForeignKey { get; }
    public Type? ForeignKeyReferencedType { get; }

    public ColumnInfo(PropertyInfo prop)
    {
        Property = prop ?? throw new ArgumentNullException(nameof(prop));
        IsPrimaryKey = prop.GetCustomAttribute<PrimaryKeyAttribute>() != null;
        IsRequired = prop.GetCustomAttribute<RequiredAttribute>() != null;
        IsAutoIncrement = prop.GetCustomAttribute<AutoIncrementAttribute>() != null;
        var foreignKey = prop.GetCustomAttribute<ForeignKeyAttribute>();
        if (foreignKey != null)
        {
            IsForeignKey = true;
            ForeignKeyReferencedType = foreignKey.ReferencedType ?? throw new InvalidOperationException($"ForeignKeyAttribute on '{Name}' must specify a referenced entity type.");
        }
    }
}

#region Attributes
    [AttributeUsage(AttributeTargets.Property)]
    public class PrimaryKeyAttribute : Attribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public class RequiredAttribute : Attribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public class AutoIncrementAttribute : Attribute { }

    [AttributeUsage(AttributeTargets.Property)]
    public sealed class ForeignKeyAttribute : Attribute
    {
        public ForeignKeyAttribute(Type referencedType)
        {
            ReferencedType = referencedType ?? throw new ArgumentNullException(nameof(referencedType));
        }

        public Type ReferencedType { get; }
    }
#endregion
