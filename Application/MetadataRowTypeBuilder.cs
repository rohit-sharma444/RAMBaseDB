namespace RAMBaseDB.Application;

using RAMBaseDB.Domain.Metadata;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

/// <summary>
/// Builds lightweight runtime types that reflect table metadata definitions so they can be hosted inside <see cref="Table{T}"/>.
/// </summary>
internal static class MetadataRowTypeBuilder
{
    private static readonly AssemblyBuilder s_assemblyBuilder;
    private static readonly ModuleBuilder s_moduleBuilder;
    private static readonly ConcurrentDictionary<string, Type> s_cachedTypes = new(StringComparer.OrdinalIgnoreCase);

    static MetadataRowTypeBuilder()
    {
        var assemblyName = new AssemblyName("RAMBaseDB.MetadataTables");
        s_assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
        s_moduleBuilder = s_assemblyBuilder.DefineDynamicModule(assemblyName.Name ?? "RAMBaseDB.MetadataTables");
    }

    public static Type GetOrCreate(string databaseName, string tableName, IReadOnlyList<FieldMetadataDocument> fields)
    {
        if (string.IsNullOrWhiteSpace(databaseName))
            throw new ArgumentException("Database name cannot be empty.", nameof(databaseName));
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name cannot be empty.", nameof(tableName));
        if (fields == null)
            throw new ArgumentNullException(nameof(fields));
        if (fields.Count == 0)
            throw new InvalidOperationException($"Table '{tableName}' does not declare any fields.");

        var cacheKey = $"{databaseName}.{tableName}";
        return s_cachedTypes.GetOrAdd(cacheKey, _ => BuildType(databaseName, tableName, fields));
    }

    private static Type BuildType(string databaseName, string tableName, IReadOnlyList<FieldMetadataDocument> fields)
    {
        var sanitizedTableName = SanitizeIdentifier(tableName);
        var typeName = $"RAMBaseDB.Metadata.{SanitizeIdentifier(databaseName)}.{sanitizedTableName}";

        var typeBuilder = s_moduleBuilder.DefineType(
            typeName,
            TypeAttributes.Public | TypeAttributes.Class);

        typeBuilder.DefineDefaultConstructor(MethodAttributes.Public);

        var usedPropertyNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var field in fields)
        {
            if (field is null)
                continue;

            var propertyName = ResolvePropertyName(field.Name, usedPropertyNames);
            var propertyType = ResolvePropertyType(field.DataType);
            var backingField = typeBuilder.DefineField($"_{propertyName}", propertyType, FieldAttributes.Private);
            var propertyBuilder = typeBuilder.DefineProperty(propertyName, PropertyAttributes.None, propertyType, null);

            var getter = typeBuilder.DefineMethod(
                $"get_{propertyName}",
                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                propertyType,
                Type.EmptyTypes);

            var getterIl = getter.GetILGenerator();
            getterIl.Emit(OpCodes.Ldarg_0);
            getterIl.Emit(OpCodes.Ldfld, backingField);
            getterIl.Emit(OpCodes.Ret);

            var setter = typeBuilder.DefineMethod(
                $"set_{propertyName}",
                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
                null,
                new[] { propertyType });

            var setterIl = setter.GetILGenerator();
            setterIl.Emit(OpCodes.Ldarg_0);
            setterIl.Emit(OpCodes.Ldarg_1);
            setterIl.Emit(OpCodes.Stfld, backingField);
            setterIl.Emit(OpCodes.Ret);

            propertyBuilder.SetGetMethod(getter);
            propertyBuilder.SetSetMethod(setter);
        }

        return typeBuilder.CreateTypeInfo()
            ?? throw new InvalidOperationException($"Failed to build runtime type for table '{tableName}'.");
    }

    private static string ResolvePropertyName(string? candidate, HashSet<string> usedNames)
    {
        var sanitized = SanitizeIdentifier(candidate);
        if (string.IsNullOrWhiteSpace(sanitized))
            sanitized = "Field";

        var resolved = sanitized;
        var suffix = 1;

        while (!usedNames.Add(resolved))
            resolved = $"{sanitized}{suffix++}";

        return resolved;
    }

    private static string SanitizeIdentifier(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;

        var trimmed = value.Trim();
        var buffer = trimmed
            .Select(ch => char.IsLetterOrDigit(ch) ? ch : '_')
            .ToArray();

        if (buffer.Length == 0)
            return string.Empty;

        if (!char.IsLetter(buffer[0]) && buffer[0] != '_')
            buffer[0] = '_';

        return new string(buffer);
    }

    private static Type ResolvePropertyType(string? dataType)
    {
        var normalized = string.IsNullOrWhiteSpace(dataType)
            ? string.Empty
            : dataType.Trim().ToUpperInvariant();

        return normalized switch
        {
            "INT" => typeof(int),
            "BIGINT" => typeof(long),
            "DECIMAL" => typeof(decimal),
            "BIT" => typeof(bool),
            "DATE" => typeof(DateTime),
            "DATETIME" => typeof(DateTime),
            "NVARCHAR" => typeof(string),
            "VARCHAR" => typeof(string),
            "UNIQUEIDENTIFIER" => typeof(Guid),
            _ => typeof(string)
        };
    }
}
