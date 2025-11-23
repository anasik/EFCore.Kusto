using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoSqlTranslatingExpressionVisitor(
    RelationalSqlTranslatingExpressionVisitorDependencies deps,
    QueryCompilationContext context,
    QueryableMethodTranslatingExpressionVisitor queryVisitor)
    : RelationalSqlTranslatingExpressionVisitor(deps, context, queryVisitor)
{
    // ------------------------------------------------------------
    // OVERRIDE: Member Access
    // ------------------------------------------------------------
    // Allow EF to translate x.Property into ColumnExpression
    protected override Expression VisitMember(MemberExpression member)
    {
        var translated = base.VisitMember(member);
        if (translated == null)
            throw new NotSupportedException($"Unsupported member: {member.Member.Name}");

        return translated;
    }

    // ------------------------------------------------------------
    // OVERRIDE: Method Calls (use base)
    // ------------------------------------------------------------
    protected override Expression VisitMethodCall(MethodCallExpression methodCall)
    {
        var translated = base.VisitMethodCall(methodCall);
        if (translated == null)
            throw new NotSupportedException($"Unsupported method call: {methodCall.Method.Name}");

        return translated;
    }

    // ------------------------------------------------------------
    // OVERRIDE: Translatable Expressions
    // ------------------------------------------------------------
    public override SqlExpression? Translate(Expression expression, bool applyDefaultTypeMapping = true)
    {
        var translated = base.Translate(expression, applyDefaultTypeMapping);

        if (translated is null)
            throw new NotSupportedException(
                $"Expression not translatable to Kusto: {expression}");

        return translated;
    }
}
