namespace RAMBaseDB.Domain.Abstractions;

using System;
using System.Collections;

public interface ITable
{
    string Name { get; set; }
    void Clear();
    void Add(object item);
    int Delete(Func<object, bool>? predicate);
    int Update(Func<object, bool>? predicate, Action<object> mutator);
    Type DataType { get; }
    IEnumerable GetRows();
}
