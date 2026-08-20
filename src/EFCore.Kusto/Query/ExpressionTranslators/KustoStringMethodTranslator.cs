using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Kusto.Query.ExpressionTranslators;

/// <summary>
/// Translates <see cref="string.IsNullOrEmpty"/> and <see cref="string.IsNullOrWhiteSpace"/>
/// directly to Kusto's <c>isempty()</c>, instead of falling through to EF's default
/// <c>IsNull(x) OR x == ""</c> expansion. That expansion relies on standard SQL null
/// propagation through <c>=</c>, which Kusto's generator doesn't model (there is no
/// relational-null rewrite for arbitrary equality comparisons here), so it collapses to
/// just <c>x == ""</c> and silently drops the null check.
/// </summary>
public sealed class KustoStringMethodTranslator(ISqlExpressionFactory sqlExpressionFactory) : IMethodCallTranslator
{
    private static readonly MethodInfo IsNullOrEmptyMethod =
        typeof(string).GetMethod(nameof(string.IsNullOrEmpty), [typeof(string)])!;

    private static readonly MethodInfo IsNullOrWhiteSpaceMethod =
        typeof(string).GetMethod(nameof(string.IsNullOrWhiteSpace), [typeof(string)])!;

    public SqlExpression? Translate(
        SqlExpression? instance,
        MethodInfo method,
        IReadOnlyList<SqlExpression> arguments,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (method == IsNullOrEmptyMethod)
            return IsEmpty(arguments[0]);

        if (method == IsNullOrWhiteSpaceMethod)
        {
            var trimmed = sqlExpressionFactory.Function(
                "trim",
                new[] { sqlExpressionFactory.Constant(@"\s+"), arguments[0] },
                nullable: true,
                argumentsPropagateNullability: new[] { false, true },
                typeof(string));

            return IsEmpty(trimmed);
        }

        return null;
    }

    private SqlExpression IsEmpty(SqlExpression argument)
        => sqlExpressionFactory.Function(
            "isempty",
            new[] { argument },
            nullable: false,
            argumentsPropagateNullability: new[] { false },
            typeof(bool));
}
