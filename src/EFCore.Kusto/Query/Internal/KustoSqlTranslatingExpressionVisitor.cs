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
    protected override Expression VisitMember(MemberExpression member)
    {
        var translated = base.VisitMember(member);
        if (translated == null)
            throw new NotSupportedException($"Unsupported member: {member.Member.Name}");

        return translated;
    }

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

        if (methodCall.Method.DeclaringType == typeof(Queryable) &&
            (methodCall.Method.Name == nameof(Queryable.Any) || methodCall.Method.Name == nameof(Queryable.All)))
        {
            var collectionExpr = methodCall.Arguments[0];
            string? columnName = null;

            if (collectionExpr is MethodCallExpression outerCall &&
                outerCall.Method.Name == nameof(Queryable.AsQueryable) &&
                outerCall.Arguments.FirstOrDefault() is MethodCallExpression innerPropertyCall &&
                innerPropertyCall.Method.Name == nameof(EF.Property) &&
                innerPropertyCall.Arguments[1] is ConstantExpression colExpr)
            {
                columnName = colExpr.Value?.ToString();
            }

            if (columnName != null)
            {
                var columnSql = deps.SqlExpressionFactory.Fragment(columnName);
                var parsedJson = deps.SqlExpressionFactory.Function(
                    "parse_json",
                    new SqlExpression[] { columnSql },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(object), dummyMapping);
                var arrayLength = deps.SqlExpressionFactory.Function(
                    "array_length",
                    new SqlExpression[] { parsedJson },
                    nullable: true,
                    argumentsPropagateNullability: new[] { true },
                    typeof(int));
                var predicate = deps.SqlExpressionFactory.GreaterThan(
                    arrayLength,
                    deps.SqlExpressionFactory.Constant(0, typeof(int))
                );
                return predicate;
            }

            return base.VisitMethodCall(methodCall);
        }

        var translated = base.VisitMethodCall(methodCall);
        if (translated == null)
            throw new NotSupportedException($"Unsupported method call: {methodCall.Method.Name}");

        return translated;
    }

    public override SqlExpression? Translate(Expression expression, bool applyDefaultTypeMapping = true)
    {
        if (TryTranslateStructuralEquality(expression, out var structuralEquality))
            return structuralEquality;

        var translated = base.Translate(expression, applyDefaultTypeMapping);

        if (translated is null)
            throw new NotSupportedException(
                $"Expression not translatable to Kusto: {expression}");

        return translated;
    }

    private bool TryTranslateStructuralEquality(Expression expression, out SqlExpression? translated)
    {
        translated = null;

        Expression? leftExpression = null;
        Expression? rightExpression = null;

        if (expression is BinaryExpression { NodeType: ExpressionType.Equal } binary)
        {
            leftExpression = binary.Left;
            rightExpression = binary.Right;
        }
        else if (expression is MethodCallExpression methodCall &&
                 methodCall.Method.Name == nameof(object.Equals) &&
                 methodCall.Arguments.Count == 2)
        {
            leftExpression = methodCall.Arguments[0];
            rightExpression = methodCall.Arguments[1];
        }

        if (leftExpression == null || rightExpression == null)
            return false;

        if (!TryGetStructuralComponents(leftExpression, out var leftComponents) ||
            !TryGetStructuralComponents(rightExpression, out var rightComponents) ||
            leftComponents.Count != rightComponents.Count)
        {
            return false;
        }

        SqlExpression? combined = null;
        for (var i = 0; i < leftComponents.Count; i++)
        {
            var left = base.Translate(leftComponents[i], applyDefaultTypeMapping: true);
            var right = base.Translate(rightComponents[i], applyDefaultTypeMapping: true);
            if (left == null || right == null)
                return false;

            var next = deps.SqlExpressionFactory.Equal(left, right);
            combined = combined == null ? next : deps.SqlExpressionFactory.AndAlso(combined, next);
        }

        translated = combined;
        return translated != null;
    }

    private static bool TryGetStructuralComponents(Expression expression, out IReadOnlyList<Expression> components)
    {
        expression = UnwrapConvert(expression);

        if (expression is NewExpression newExpression)
        {
            components = newExpression.Arguments;
            return true;
        }

        if (expression is MemberInitExpression memberInit)
        {
            components = memberInit.Bindings
                .OfType<MemberAssignment>()
                .Select(binding => binding.Expression)
                .ToArray();
            return true;
        }

        components = Array.Empty<Expression>();
        return false;
    }

    private static Expression UnwrapConvert(Expression expression)
    {
        while (expression is UnaryExpression { NodeType: ExpressionType.Convert } unary)
        {
            expression = unary.Operand;
        }

        return expression;
    }
}

