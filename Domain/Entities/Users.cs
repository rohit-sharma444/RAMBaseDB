namespace RAMBaseDB.Domain.Entities;

using System.Collections.Concurrent;
using System.Collections.Generic;

/// <summary>
/// Represents a thread-safe collection of user objects, indexed by their unique integer identifiers.
/// </summary>
/// <remarks>The Users class extends ConcurrentDictionary to provide concurrent access and modification of user
/// data. It is suitable for scenarios where multiple threads need to read and update user information without explicit
/// synchronization. All standard dictionary operations are available, and thread safety is guaranteed for all public
/// methods.</remarks>
public class Users : ConcurrentDictionary<int, User>
{
    public Users()
    {
    }

    public Users(IEnumerable<KeyValuePair<int, User>> collection) : base(collection)
    {
    }
}
