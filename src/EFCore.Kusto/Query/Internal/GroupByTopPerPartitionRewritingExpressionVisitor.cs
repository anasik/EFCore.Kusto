using System.Linq.Expressions;
using System.Reflection;

namespace EFCore.Kusto.Query.Internal;

internal sealed class GroupByTopPerPartitionRewritingExpressionVisitor : ExpressionVisitor
{
    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        var visited = (MethodCallExpression)base.VisitMethodCall(node);

        if (!IsMethod(visited, typeof(Queryable), nameof(Queryable.Select), 2))
            return visited;

        if (StripQuotes(visited.Arguments[0]) is not MethodCallExpression groupByCall ||
            !IsMethod(groupByCall, typeof(Queryable), nameof(Queryable.GroupBy), 2))
        {
            return visited;
        }

        if (StripQuotes(visited.Arguments[1]) is not LambdaExpression selector)
            return visited;

        if (!TryMatchTopPerGroupSelector(selector, out var orderByDescendingCall))
            return visited;

        var source = groupByCall.Arguments[0];
        var keySelector = (LambdaExpression)StripQuotes(groupByCall.Arguments[1]);
        var orderByLambda = (LambdaExpression)StripQuotes(orderByDescendingCall.Arguments[1]);
        var entityType = keySelector.Parameters[0].Type;
        var keyType = keySelector.ReturnType;
        var orderType = orderByLambda.ReturnType;
        var joinKeyType = typeof(JoinKey<,>).MakeGenericType(keyType, orderType);
        var constructor = joinKeyType.GetConstructor(new[] { keyType, orderType })
            ?? throw new InvalidOperationException("Could not resolve join key constructor.");
        var keyProperty = joinKeyType.GetProperty(nameof(JoinKey<object, object>.Key))
            ?? throw new InvalidOperationException("Could not resolve join key property.");
        var orderProperty = joinKeyType.GetProperty(nameof(JoinKey<object, object>.Order))
            ?? throw new InvalidOperationException("Could not resolve join order property.");

        var groupingParameter = Expression.Parameter(typeof(IGrouping<,>).MakeGenericType(keyType, entityType), "group");
        var maxCall = Expression.Call(
            EnumerableMethods.MaxWithSelector.MakeGenericMethod(entityType, orderType),
            groupingParameter,
            StripQuotes(orderByDescendingCall.Arguments[1]));
        var groupedSummaryProjection = CreateJoinKeyNewExpression(
            constructor,
            keyProperty,
            Expression.Property(groupingParameter, nameof(IGrouping<object, object>.Key)),
            orderProperty,
            maxCall);
        var groupedSummarySelector = Expression.Lambda(groupedSummaryProjection, groupingParameter);
        var groupedSummary = Expression.Call(
            QueryableMethods.Select.MakeGenericMethod(groupingParameter.Type, joinKeyType),
            groupByCall,
            Expression.Quote(groupedSummarySelector));

        var entityParameter = Expression.Parameter(entityType, orderByLambda.Parameters[0].Name ?? "entity");
        var outerJoinKeySelector = Expression.Lambda(
            CreateJoinKeyNewExpression(
                constructor,
                keyProperty,
                Replace(keySelector.Parameters[0], entityParameter, keySelector.Body),
                orderProperty,
                Replace(orderByLambda.Parameters[0], entityParameter, orderByLambda.Body)),
            entityParameter);

        var summaryParameter = Expression.Parameter(joinKeyType, "summary");
        var innerJoinKeySelector = Expression.Lambda(
            CreateJoinKeyNewExpression(
                constructor,
                keyProperty,
                Expression.Property(summaryParameter, keyProperty),
                orderProperty,
                Expression.Property(summaryParameter, orderProperty)),
            summaryParameter);

        var resultSelector = Expression.Lambda(entityParameter, entityParameter, summaryParameter);

        return Expression.Call(
            QueryableMethods.Join.MakeGenericMethod(entityType, joinKeyType, joinKeyType, entityType),
            source,
            groupedSummary,
            Expression.Quote(outerJoinKeySelector),
            Expression.Quote(innerJoinKeySelector),
            Expression.Quote(resultSelector));
    }

    private static NewExpression CreateJoinKeyNewExpression(
        ConstructorInfo constructor,
        PropertyInfo keyProperty,
        Expression keyValue,
        PropertyInfo orderProperty,
        Expression orderValue)
    {
        return Expression.New(constructor, new[] { keyValue, orderValue }, new MemberInfo[] { keyProperty, orderProperty });
    }

    private static bool TryMatchTopPerGroupSelector(LambdaExpression selector, out MethodCallExpression orderByDescendingCall)
    {
        orderByDescendingCall = null!;

        if (selector.Body is not MethodCallExpression firstCall)
            return false;

        if (!IsMethod(firstCall, typeof(Queryable), nameof(Queryable.First), 1) &&
            !IsMethod(firstCall, typeof(Queryable), nameof(Queryable.FirstOrDefault), 1) &&
            !IsMethod(firstCall, typeof(Enumerable), nameof(Enumerable.First), 1) &&
            !IsMethod(firstCall, typeof(Enumerable), nameof(Enumerable.FirstOrDefault), 1))
        {
            return false;
        }

        if (StripQuotes(firstCall.Arguments[0]) is not MethodCallExpression orderByCall)
            return false;

        if (!IsMethod(orderByCall, typeof(Queryable), nameof(Queryable.OrderByDescending), 2) &&
            !IsMethod(orderByCall, typeof(Enumerable), nameof(Enumerable.OrderByDescending), 2))
        {
            return false;
        }

        if (!ReferencesParameter(orderByCall.Arguments[0], selector.Parameters[0]))
            return false;

        if (StripQuotes(orderByCall.Arguments[1]) is not LambdaExpression)
            return false;

        orderByDescendingCall = orderByCall;
        return true;
    }

    private static bool ReferencesParameter(Expression expression, ParameterExpression parameter)
    {
        var found = false;
        new ParameterReferenceVisitor(parameter, () => found = true).Visit(expression);
        return found;
    }

    private static Expression Replace(ParameterExpression source, Expression replacement, Expression target)
        => new ParameterReplaceVisitor(source, replacement).Visit(target)!;

    private static Expression StripQuotes(Expression expression)
    {
        while (expression.NodeType == ExpressionType.Quote)
        {
            expression = ((UnaryExpression)expression).Operand;
        }

        if (expression is ConstantExpression constant && constant.Value is LambdaExpression lambda)
        {
            return lambda;
        }

        return expression;
    }

    private static bool IsMethod(MethodCallExpression expression, Type declaringType, string name, int parameterCount)
        => expression.Method.DeclaringType == declaringType
           && expression.Method.Name == name
           && expression.Arguments.Count == parameterCount;

    private static class QueryableMethods
    {
        public static readonly MethodInfo Select = GetQueryableMethod(nameof(Queryable.Select), 2);
        public static readonly MethodInfo Join = typeof(Queryable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(Queryable.Join)
                         && m.IsGenericMethodDefinition
                         && m.GetGenericArguments().Length == 4
                         && m.GetParameters().Length == 5);

        private static MethodInfo GetQueryableMethod(string name, int parameterCount)
            => typeof(Queryable)
                .GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(m => m.Name == name
                             && m.IsGenericMethodDefinition
                             && m.GetParameters().Length == parameterCount
                             && GetLambdaArity(m.GetParameters().Last().ParameterType) == 2);

        private static int GetLambdaArity(Type parameterType)
        {
            if (!parameterType.IsGenericType)
                return -1;

            var parameterArg = parameterType.GetGenericArguments().SingleOrDefault();
            return parameterArg?.IsGenericType == true ? parameterArg.GetGenericArguments().Length : -1;
        }
    }

    private static class EnumerableMethods
    {
        public static readonly MethodInfo MaxWithSelector = typeof(Enumerable)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == nameof(Enumerable.Max)
                         && m.IsGenericMethodDefinition
                         && m.GetGenericArguments().Length == 2
                         && m.GetParameters().Length == 2);
    }

    private sealed class JoinKey<TKey, TOrder>
    {
        public JoinKey(TKey key, TOrder order)
        {
            Key = key;
            Order = order;
        }

        public TKey Key { get; }
        public TOrder Order { get; }
    }

    private sealed class ParameterReferenceVisitor(ParameterExpression target, Action onFound) : ExpressionVisitor
    {
        public override Expression? Visit(Expression? node)
        {
            if (node == target)
            {
                onFound();
            }

            return base.Visit(node);
        }
    }

    private sealed class ParameterReplaceVisitor(ParameterExpression source, Expression replacement) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node)
            => node == source ? replacement : base.VisitParameter(node);
    }
}
