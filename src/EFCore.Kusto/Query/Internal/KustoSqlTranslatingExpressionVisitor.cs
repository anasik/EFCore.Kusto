using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

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
                    return BuildArrayMembershipCheck(sqlCollection, sqlValue, dummyMapping, negate: false);
                }
            }
        }

        if (methodCall.Method.DeclaringType == typeof(Queryable) &&
            (methodCall.Method.Name == nameof(Queryable.Any) || methodCall.Method.Name == nameof(Queryable.All)))
        {
            var collectionExpr = methodCall.Arguments[0];
            MethodCallExpression? propertyAccessCall = null;
            string? columnName = null;

            if (collectionExpr is MethodCallExpression outerCall &&
                outerCall.Method.Name == nameof(Queryable.AsQueryable) &&
                outerCall.Arguments.FirstOrDefault() is MethodCallExpression innerPropertyCall &&
                innerPropertyCall.Method.Name == nameof(EF.Property) &&
                innerPropertyCall.Arguments[1] is ConstantExpression colExpr)
            {
                propertyAccessCall = innerPropertyCall;
                columnName = colExpr.Value?.ToString();
            }

            if (columnName != null && propertyAccessCall != null
                && Visit(propertyAccessCall) is SqlExpression sqlCollection)
            {
                bool isAny = methodCall.Method.Name == nameof(Queryable.Any);

                // Supported: Any(a => a == x || a == y || ...), All(a => a != x && a != y && ...).
                // Anything else (mixed &&/||, ranges, method calls) throws below.
                var predicateLambda = methodCall.Arguments.Count == 2
                    ? methodCall.Arguments[1] switch
                    {
                        LambdaExpression l => l,
                        UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression ql } => ql,
                        _ => null
                    }
                    : null;

                if (predicateLambda != null)
                {
                    var leaves = new List<SqlExpression>();
                    if (TryCollectArrayPredicateLeaves(
                            predicateLambda.Body, predicateLambda.Parameters[0], isAny, sqlCollection, dummyMapping, leaves))
                    {
                        var combined = leaves[0];
                        for (int i = 1; i < leaves.Count; i++)
                            combined = isAny
                                ? deps.SqlExpressionFactory.OrElse(combined, leaves[i])
                                : deps.SqlExpressionFactory.AndAlso(combined, leaves[i]);
                        return combined;
                    }

                    throw new NotSupportedException(
                        $"Unsupported {(isAny ? "Any" : "All")} predicate over array column '{columnName}': only "
                        + (isAny ? "disjunctions (||) of equality" : "conjunctions (&&) of inequality")
                        + " checks against a constant are supported.");
                }

                // Any() with no predicate: array non-empty check.
                var parsedJson = deps.SqlExpressionFactory.Function(
                    "parse_json",
                    new SqlExpression[] { sqlCollection },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(object), dummyMapping);
                var arrayLength = deps.SqlExpressionFactory.Function(
                    "array_length",
                    new SqlExpression[] { parsedJson },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(int));
                return deps.SqlExpressionFactory.GreaterThan(
                    arrayLength, deps.SqlExpressionFactory.Constant(0, typeof(int)));
            }

            return base.VisitMethodCall(methodCall);
        }

        var translated = base.VisitMethodCall(methodCall);
        if (translated == null)
            throw new NotSupportedException($"Unsupported method call: {methodCall.Method.Name}");

        return translated;
    }

    // ------------------------------------------------------------
    // Array-membership helpers, shared by Contains and Any/All
    // ------------------------------------------------------------

    /// <summary>
    /// Collects <c>parameter == const</c> (Any) / <c>!= const</c> (All) leaves,
    /// recursing through OrElse (Any) / AndAlso (All). Returns false — not a
    /// partial result — on any other shape.
    /// </summary>
    private bool TryCollectArrayPredicateLeaves(
        Expression node,
        ParameterExpression parameter,
        bool isAny,
        SqlExpression sqlCollection,
        RelationalTypeMapping? dummyMapping,
        List<SqlExpression> leaves)
    {
        if (node is UnaryExpression { NodeType: ExpressionType.Convert } convert)
            node = convert.Operand;

        var combinator = isAny ? ExpressionType.OrElse : ExpressionType.AndAlso;
        if (node is BinaryExpression combinedNode && combinedNode.NodeType == combinator)
        {
            return TryCollectArrayPredicateLeaves(combinedNode.Left, parameter, isAny, sqlCollection, dummyMapping, leaves)
                && TryCollectArrayPredicateLeaves(combinedNode.Right, parameter, isAny, sqlCollection, dummyMapping, leaves);
        }

        var leafOperator = isAny ? ExpressionType.Equal : ExpressionType.NotEqual;
        if (node is BinaryExpression leaf && leaf.NodeType == leafOperator)
        {
            Expression? constSide = leaf.Left == parameter ? leaf.Right : leaf.Right == parameter ? leaf.Left : null;
            if (constSide != null && Visit(constSide) is SqlExpression sqlValue)
            {
                leaves.Add(BuildArrayMembershipCheck(sqlCollection, sqlValue, dummyMapping, negate: !isAny));
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// <c>array_index_of(parse_json(collection), value) &lt;&gt; -1</c> (<c>== -1</c>
    /// negated) — shared by Contains and every Any/All leaf.
    /// </summary>
    private SqlExpression BuildArrayMembershipCheck(
        SqlExpression sqlCollection, SqlExpression sqlValue, RelationalTypeMapping? dummyMapping, bool negate)
    {
        // Not inferred jointly with sqlCollection: its mapping is a List<T><->JSON
        // converter, and applying that to a scalar throws (verified: InvalidCastException).
        sqlValue = deps.SqlExpressionFactory.ApplyDefaultTypeMapping(sqlValue);

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

        var minusOne = deps.SqlExpressionFactory.Constant(-1, typeof(int));
        return negate
            ? deps.SqlExpressionFactory.Equal(arrayIndexOf, minusOne)
            : deps.SqlExpressionFactory.NotEqual(arrayIndexOf, minusOne);
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