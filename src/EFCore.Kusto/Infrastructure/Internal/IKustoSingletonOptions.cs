using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Kusto.Infrastructure.Internal;

/// <summary>
/// This is an internal API that supports the Entity Framework Core infrastructure and not subject to
/// the same compatibility standards as public APIs. It may be changed or removed without notice in
/// any release. You should only use it directly in your code with extreme caution and knowing that
/// doing so can result in application failures when updating to a new Entity Framework Core release.
/// </summary>
public interface IKustoSingletonOptions : ISingletonOptions
{
    /// <summary>
    /// Whether <c>x.Field == null</c> / <c>!= null</c> on a string-typed operand is generated as
    /// <c>isempty()</c>/<c>isnotempty()</c> instead of <c>isnull()</c>/<c>isnotnull()</c>.
    /// </summary>
    bool TreatNullAsEmpty { get; }
}
