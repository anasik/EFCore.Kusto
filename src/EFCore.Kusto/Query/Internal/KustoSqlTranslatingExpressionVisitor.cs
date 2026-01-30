using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
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
        var dummyMapping = deps.SqlExpressionFactory
            .ApplyDefaultTypeMapping(deps.SqlExpressionFactory.Constant("", typeof(string)))
            .TypeMapping;
        if (methodCall.Method.IsGenericMethod &&
            methodCall.Method.GetGenericMethodDefinition() == QueryableMethods.Contains)
        {
            var collectionExpr = methodCall.Arguments[0];
            var valueExpr = methodCall.Arguments[1];

            if (collectionExpr is MethodCallExpression inner
                && inner.Method.Name == nameof(Queryable.AsQueryable)
                && inner.Arguments.Count == 1)
            {
                var rawCollection = inner.Arguments[0];

                if (Visit(rawCollection) is SqlExpression sqlCollection &&
                    Visit(valueExpr) is SqlExpression sqlValue)
                {
                    var stringMapping = ExpressionExtensions.InferTypeMapping(sqlCollection, sqlValue);
                    sqlCollection = deps.SqlExpressionFactory.ApplyTypeMapping(sqlCollection, stringMapping);
                    sqlValue = deps.SqlExpressionFactory.ApplyTypeMapping(sqlValue, dummyMapping);
                    var parsedJson = deps.SqlExpressionFactory.Function(
                        "parse_json",
                        new SqlExpression[] { sqlCollection },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true },
                        typeof(object), dummyMapping);

                    var arrayIndexOf = deps.SqlExpressionFactory.Function(
                        "array_index_of",
                        new SqlExpression[] { parsedJson, sqlValue },
                        nullable: true,
                        argumentsPropagateNullability: new[] { true, true },
                        typeof(int));

                    var notEqualsMinusOne = deps.SqlExpressionFactory.NotEqual(
                        arrayIndexOf,
                        deps.SqlExpressionFactory.Constant(-1, typeof(int))
                    );

                    return notEqualsMinusOne;
                }
            }
        }

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