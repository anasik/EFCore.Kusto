using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Kusto.Query.ExpressionTranslators;

/// <summary>
/// Translates date/time component members to their Kusto equivalents.
/// Without this, nothing translates date parts (the relational base provider supplies
/// none — every provider brings its own), so any predicate containing one fails with
/// <c>NotSupportedException: Expression not translatable to Kusto: ...</c>. That is what
/// OData <c>$filter</c> date comparisons hit: they are rewritten as
/// <c>Year * 10000 + Month * 100 + Day &gt; @param</c>.
/// Covers <see cref="DateTime"/>, <see cref="DateOnly"/> and <see cref="DateTimeOffset"/>:
/// the three declare separate <c>Year</c>/<c>Month</c>/<c>Day</c> members, so matching on
/// <see cref="MemberInfo.DeclaringType"/> alone would silently miss the other two and
/// leave them throwing. Kusto has a single <c>datetime</c> scalar (UTC, no offset), so all
/// three map onto the same functions.
/// </summary>
public sealed class KustoDateTimeMemberTranslator(ISqlExpressionFactory sqlExpressionFactory) : IMemberTranslator
{
    public SqlExpression? Translate(
        SqlExpression? instance,
        MemberInfo member,
        Type returnType,
        IDiagnosticsLogger<DbLoggerCategory.Query> logger)
    {
        if (instance is null || !IsDateTimeType(member.DeclaringType))
            return null;

        return member.Name switch
        {
            nameof(DateTime.Year) => Function("getyear", instance, returnType),
            nameof(DateTime.Month) => Function("monthofyear", instance, returnType),
            nameof(DateTime.Day) => Function("dayofmonth", instance, returnType),
            nameof(DateTime.DayOfYear) => Function("dayofyear", instance, returnType),
            nameof(DateTime.Hour) => Function("hourofday", instance, returnType),
            nameof(DateTime.Minute) => DatePart("Minute", instance, returnType),
            nameof(DateTime.Second) => DatePart("Second", instance, returnType),
            nameof(DateTime.Millisecond) => DatePart("Millisecond", instance, returnType),
            nameof(DateTime.Date) => Function("startofday", instance, returnType),
            nameof(DateTime.DayOfWeek) => DayOfWeek(instance, returnType),
            _ => null
        };
    }

    /// <summary>
    /// <c>datetime_diff("day", x, startofweek(x))</c> — days elapsed since the start of
    /// the week. Kusto's <c>startofweek()</c> is Sunday-based, so this yields 0–6 with
    /// Sunday = 0, matching <see cref="System.DayOfWeek"/> exactly. Kusto's own
    /// <c>dayofweek()</c> is not used: it returns a <c>timespan</c>, not an int, so it
    /// would need dividing by <c>1d</c> before it could be compared to the enum's value.
    /// </summary>
    private SqlExpression DayOfWeek(SqlExpression instance, Type returnType)
        => sqlExpressionFactory.Function(
            "datetime_diff",
            new[] { sqlExpressionFactory.Constant("day"), instance, Function("startofweek", instance, instance.Type) },
            nullable: true,
            argumentsPropagateNullability: new[] { false, true, true },
            returnType);

    private static bool IsDateTimeType(Type? declaringType)
        => declaringType == typeof(DateTime)
            || declaringType == typeof(DateOnly)
            || declaringType == typeof(DateTimeOffset);

    private SqlExpression Function(string name, SqlExpression instance, Type returnType)
        => sqlExpressionFactory.Function(
            name,
            new[] { instance },
            nullable: true,
            argumentsPropagateNullability: new[] { true },
            returnType);

    private SqlExpression DatePart(string part, SqlExpression instance, Type returnType)
        => sqlExpressionFactory.Function(
            "datetime_part",
            new[] { sqlExpressionFactory.Constant(part), instance },
            nullable: true,
            argumentsPropagateNullability: new[] { false, true },
            returnType);
}
