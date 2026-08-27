using System.Reflection;
using EFCore.Kusto.Query.Internal;
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

    private static readonly MethodInfo ContainsMethod =
        typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;

    private static readonly MethodInfo StartsWithMethod =
        typeof(string).GetMethod(nameof(string.StartsWith), [typeof(string)])!;

    private static readonly MethodInfo EndsWithMethod =
        typeof(string).GetMethod(nameof(string.EndsWith), [typeof(string)])!;

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

        // _cs ("case sensitive"): Kusto's plain contains/startswith/endswith
        // are case-insensitive by default, unlike C#'s Contains/StartsWith/EndsWith.
        if (method == ContainsMethod)
            return StringOperator("contains_cs", instance!, arguments[0]);

        if (method == StartsWithMethod)
            return StringOperator("startswith_cs", instance!, arguments[0]);

        if (method == EndsWithMethod)
            return StringOperator("endswith_cs", instance!, arguments[0]);

        return null;
    }

    private SqlExpression StringOperator(string kustoOperator, SqlExpression operand, SqlExpression pattern)
    {
        // ApplyTypeMapping/ApplyDefaultTypeMapping only know how to re-mount
        // built-in SqlExpression subtypes, so a custom node has to have its
        // mappings resolved and attached here instead of relying on them.
        var stringMapping = ExpressionExtensions.InferTypeMapping(operand, pattern);
        operand = sqlExpressionFactory.ApplyTypeMapping(operand, stringMapping);
        pattern = sqlExpressionFactory.ApplyTypeMapping(pattern, stringMapping);

        var boolMapping = sqlExpressionFactory.ApplyDefaultTypeMapping(sqlExpressionFactory.Constant(true)).TypeMapping;
        return new KustoStringOperatorExpression(kustoOperator, operand, pattern, boolMapping);
    }

    private SqlExpression IsEmpty(SqlExpression argument)
        => sqlExpressionFactory.Function(
            "isempty",
            new[] { argument },
            nullable: false,
            argumentsPropagateNullability: new[] { false },
            typeof(bool));
}
