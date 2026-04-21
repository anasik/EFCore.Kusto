using System.Globalization;
using System.Linq.Expressions;
using EFCore.Kusto.Query.Internal;
using Kusto.Cloud.Platform.Utils;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Query;

public sealed class KustoQuerySqlGenerator(QuerySqlGeneratorDependencies deps) : QuerySqlGenerator(deps)
{
    private int _selectDepth;
    private readonly OuterApplyPartitionHandler _outerApplyHandler = new();
    private readonly HashSet<string> _currentJoinLeftAliases = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _currentJoinRightAliases = new(StringComparer.OrdinalIgnoreCase);

    protected override Expression VisitSelect(SelectExpression select)
    {
        var isNested = _selectDepth > 0;
        _selectDepth++;

        if (isNested)
        {
            Sql.Append("(");
            Sql.AppendLine();
        }

        WriteFrom(select);
        WriteWhere(select);

        if (select.GroupBy.Count > 0)
        {
            WriteSummarize(select);
            WriteHaving(select);
            WriteOrderBy(select);
            WriteSkip(select);
            WriteTake(select);
        }
        else
        {
            WriteOrderBy(select);
            WriteProjection(select);
            WriteSkip(select);
            WriteTake(select);
        }

        if (isNested)
        {
            Sql.AppendLine();
            Sql.Append(")");
        }

        _selectDepth--;
        return select;
    }

    private void WriteFrom(SelectExpression select)
    {
        if (select.Tables.Count == 0)
        {
            return;
        }

        if (select.Tables.Count == 1)
        {
            WriteSingleFrom(select.Tables[0]);
            return;
        }

        WriteJoinedFrom(select);
    }

    private void WriteSingleFrom(TableExpressionBase table)
    {
        switch (table)
        {
            case TableExpression t:
                Sql.Append(t.Table.Name);
                break;

            case FromSqlExpression f:
                Sql.Append("(");
                Sql.Append(f.Sql);
                Sql.Append(")");
                break;

            case SelectExpression nested:
                VisitSelect(nested);
                break;

            case LeftJoinExpression left:
                WriteSingleFrom(left.Table);
                break;

            case InnerJoinExpression inner:
                WriteSingleFrom(inner.Table);
                break;

            case OuterApplyExpression outerApply:
                WriteSingleFrom(outerApply.Table);
                break;

            case CrossApplyExpression crossApply:
                WriteSingleFrom(crossApply.Table);
                break;

            default:
                throw new NotSupportedException($"Unsupported table expression: {table.GetType().Name}");
        }
    }

    private void WriteJoinedFrom(SelectExpression select)
    {
        var left = select.Tables[0];
        WriteSingleFrom(left);

        for (var i = 1; i < select.Tables.Count; i++)
        {
            var right = select.Tables[i];

            if (right is LeftJoinExpression leftJoin)
            {
                CaptureJoinAliases(leftJoin.JoinPredicate);
                Sql.AppendLine();
                Sql.Append("| join kind=leftouter (");
                WriteSingleFrom(leftJoin.Table);
                Sql.Append(") on ");
                WriteJoinPredicate(leftJoin.JoinPredicate);
            }
            else if (right is InnerJoinExpression innerJoin)
            {
                CaptureJoinAliases(innerJoin.JoinPredicate);
                Sql.AppendLine();
                Sql.Append("| join kind=inner (");
                WriteSingleFrom(innerJoin.Table);
                Sql.Append(") on ");
                WriteJoinPredicate(innerJoin.JoinPredicate);
            }
            else if (_outerApplyHandler.IsApplyJoin(right))
            {
                Sql.AppendLine();
                Sql.Append("| join kind=leftouter (");
                _outerApplyHandler.ProcessApplyJoin(right, Sql, WriteSingleFrom, WriteJoinPredicate);
            }
            else
            {
                throw new NotSupportedException($"Unsupported join expression: {right.GetType().Name}");
            }
        }
    }

    private void CaptureJoinAliases(SqlExpression predicate)
    {
        _currentJoinLeftAliases.Clear();
        _currentJoinRightAliases.Clear();
        CollectJoinAliases(predicate);
    }

    private void CollectJoinAliases(SqlExpression expression)
    {
        if (expression is SqlBinaryExpression binary)
        {
            if (binary.OperatorType == ExpressionType.Equal)
            {
                var left = UnwrapJoinColumn(binary.Left);
                var right = UnwrapJoinColumn(binary.Right);
                if (left != null && right != null)
                {
                    if (!string.IsNullOrWhiteSpace(left.TableAlias))
                        _currentJoinLeftAliases.Add(left.TableAlias);
                    if (!string.IsNullOrWhiteSpace(right.TableAlias))
                        _currentJoinRightAliases.Add(right.TableAlias);
                    return;
                }
            }

            CollectJoinAliases(binary.Left);
            CollectJoinAliases(binary.Right);
        }
        else if (expression is SqlUnaryExpression unary)
        {
            CollectJoinAliases(unary.Operand);
        }
    }

    private void WriteJoinPredicate(SqlExpression predicate)
        => WriteJoinPredicate(predicate, parentOperator: null);

    private void WriteJoinPredicate(SqlExpression predicate, ExpressionType? parentOperator)
    {
        if (predicate is SqlBinaryExpression binary)
        {
            if (binary.OperatorType == ExpressionType.Equal)
            {
                WriteJoinSide(binary.Left, isLeft: true);
                Sql.Append(" == ");
                WriteJoinSide(binary.Right, isLeft: false);
                return;
            }

            if (binary.OperatorType is ExpressionType.And or ExpressionType.AndAlso or ExpressionType.Or or ExpressionType.OrElse)
            {
                var currentOperator = NormalizeLogicalOperator(binary.OperatorType);
                var requiresParentheses = parentOperator != null && parentOperator != currentOperator;
                if (requiresParentheses)
                    Sql.Append("(");

                WriteJoinPredicate(binary.Left, currentOperator);
                Sql.Append(currentOperator == ExpressionType.AndAlso ? " and " : " or ");
                WriteJoinPredicate(binary.Right, currentOperator);

                if (requiresParentheses)
                    Sql.Append(")");
                return;
            }
        }

        if (predicate is SqlUnaryExpression unary)
        {
            if (unary.OperatorType == ExpressionType.Equal)
            {
                Sql.Append("isnull(");
                WriteJoinOperand(unary.Operand);
                Sql.Append(")");
                return;
            }

            if (unary.OperatorType == ExpressionType.NotEqual)
            {
                Sql.Append("isnotnull(");
                WriteJoinOperand(unary.Operand);
                Sql.Append(")");
                return;
            }

            if (unary.OperatorType == ExpressionType.Not && unary.Operand is SqlExpression operand)
            {
                Sql.Append("not (");
                WriteJoinPredicate(operand, parentOperator: null);
                Sql.Append(")");
                return;
            }
        }

        throw new NotSupportedException($"Unsupported join predicate: {predicate.GetType().Name}");
    }

    private static ExpressionType NormalizeLogicalOperator(ExpressionType operatorType)
        => operatorType is ExpressionType.And or ExpressionType.AndAlso ? ExpressionType.AndAlso : ExpressionType.OrElse;

    private void WriteJoinOperand(SqlExpression expression)
    {
        var column = UnwrapJoinColumn(expression);
        if (column != null)
        {
            var isLeft = string.IsNullOrWhiteSpace(column.TableAlias) || !_currentJoinRightAliases.Contains(column.TableAlias);
            WriteJoinSide(column, isLeft);
            return;
        }

        Visit(expression);
    }

    private static ColumnExpression? UnwrapJoinColumn(SqlExpression expression)
    {
        if (expression is ColumnExpression column)
            return column;

        if (expression is SqlUnaryExpression unary && unary.OperatorType == ExpressionType.Convert)
            return UnwrapJoinColumn(unary.Operand);

        return null;
    }

    private void WriteJoinSide(SqlExpression expr, bool isLeft)
    {
        if (expr is ColumnExpression c)
        {
            Sql.Append(isLeft ? "$left." : "$right.");
            Sql.Append(c.Name);
            return;
        }

        if (expr is SqlUnaryExpression unary && unary.OperatorType == ExpressionType.Convert)
        {
            WriteJoinSide(unary.Operand, isLeft);
            return;
        }

        throw new NotSupportedException($"Unsupported join key expression: {expr.GetType().Name}");
    }

    private void WriteWhere(SelectExpression select)
    {
        if (select.Predicate == null)
            return;

        if (select.Predicate is SqlBinaryExpression predicate &&
            predicate.Left is ColumnExpression leftColumn &&
            leftColumn.Name == "row" &&
            predicate.OperatorType == ExpressionType.LessThanOrEqual)
        {
            return;
        }

        var predicateToRender = _outerApplyHandler.IsActive
            ? _outerApplyHandler.GetCleanedPredicate(select.Predicate)
            : select.Predicate;

        if (predicateToRender == null)
            return;

        Sql.AppendLine();
        Sql.Append("| where ");
        Visit(predicateToRender);
    }

    private void WriteSummarize(SelectExpression select)
    {
        if (select.Projection.Count == 0)
            throw new NotSupportedException("Grouped queries must project at least one grouping key or aggregate.");

        var groupingProjections = new List<(SqlExpression Expression, string? Alias)>();
        foreach (var groupExpression in select.GroupBy)
        {
            var alias = select.Projection.FirstOrDefault(projection => ExpressionsEquivalent(projection.Expression, groupExpression)).Alias;
            groupingProjections.Add((groupExpression, alias));
        }

        var aggregateProjections = new List<ProjectionExpression>();
        foreach (var projection in select.Projection)
        {
            if (IsAggregateExpression(projection.Expression))
            {
                aggregateProjections.Add(projection);
                continue;
            }

            var projectionExpression = UnwrapConversions(projection.Expression);
            if (projectionExpression is ColumnExpression)
                continue;

            if (groupingProjections.Any(grouping => ExpressionsEquivalent(grouping.Expression, projection.Expression)))
                continue;

            // EF can introduce additional grouped-key projection wrappers. As long as summarize is driven by
            // the flattened GroupBy terms and supported aggregate projections, the generated KQL remains correct.
            continue;
        }

        Sql.AppendLine();
        Sql.Append("| summarize ");

        var wroteAny = false;
        for (var i = 0; i < aggregateProjections.Count; i++)
        {
            if (wroteAny)
                Sql.Append(", ");

            WriteAggregateProjection(aggregateProjections[i]);
            wroteAny = true;
        }

        if (groupingProjections.Count > 0)
        {
            if (wroteAny)
                Sql.Append(" by ");

            for (var i = 0; i < groupingProjections.Count; i++)
            {
                if (i > 0)
                    Sql.Append(", ");

                WriteGroupedKey(groupingProjections[i]);
            }
        }
    }

    private void WriteGroupedKey((SqlExpression Expression, string? Alias) grouping)
    {
        if (!string.IsNullOrWhiteSpace(grouping.Alias))
        {
            Sql.Append(grouping.Alias);
            Sql.Append(" = ");
        }

        Visit(UnwrapConversions(grouping.Expression));
    }

    private void WriteAggregateProjection(ProjectionExpression projection)
    {
        if (!string.IsNullOrWhiteSpace(projection.Alias))
        {
            Sql.Append(projection.Alias);
            Sql.Append(" = ");
        }

        WriteAggregateExpression(projection.Expression);
    }

    private void WriteAggregateExpression(SqlExpression expression)
    {
        if (!TryGetAggregateFunction(expression, out var function) || !TryGetAggregateFunctionName(function.Name, out var kustoName))
            throw new NotSupportedException($"Unsupported grouped aggregate expression: {expression.GetType().Name}");

        Sql.Append(kustoName);
        Sql.Append("(");

        var arguments = function.Arguments.Where(argument => !IsStarFragment(argument)).ToList();
        for (var i = 0; i < arguments.Count; i++)
        {
            if (i > 0)
                Sql.Append(", ");

            Visit(UnwrapConversions(arguments[i]));
        }

        Sql.Append(")");
    }

    private void WriteHaving(SelectExpression select)
    {
        if (select.Having == null)
            return;

        Sql.AppendLine();
        Sql.Append("| where ");
        Visit(select.Having);
    }

    protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
    {
        if (sqlUnaryExpression.OperatorType == ExpressionType.Equal)
        {
            Sql.Append(" isnull(");
            Visit(sqlUnaryExpression.Operand);
            Sql.Append(") ");
            return sqlUnaryExpression;
        }

        if (sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
        {
            Sql.Append(" isnotnull(");
            Visit(sqlUnaryExpression.Operand);
            Sql.Append(") ");
            return sqlUnaryExpression;
        }

        if (sqlUnaryExpression.OperatorType == ExpressionType.Not)
        {
            Sql.Append("not (");
            Visit(sqlUnaryExpression.Operand);
            Sql.Append(")");
            return sqlUnaryExpression;
        }

        return base.VisitSqlUnary(sqlUnaryExpression);
    }

    protected override Expression VisitSqlBinary(SqlBinaryExpression sqlBinaryExpression)
    {
        var left = sqlBinaryExpression.Left;
        var right = sqlBinaryExpression.Right;
        var operatorType = sqlBinaryExpression.OperatorType;

        if (left.TypeMapping is StringTypeMapping && right.TypeMapping is StringTypeMapping)
        {
            var op = operatorType switch
            {
                ExpressionType.GreaterThan => "> 0 ",
                ExpressionType.GreaterThanOrEqual => ">= 0 ",
                ExpressionType.LessThan => "< 0 ",
                ExpressionType.LessThanOrEqual => "<= 0 ",
                _ => null
            };

            if (op != null)
            {
                Sql.Append(" strcmp(");
                Visit(left);
                Sql.Append(", ");
                Visit(right);
                Sql.Append($") {op}");
                return sqlBinaryExpression;
            }
        }

        return base.VisitSqlBinary(sqlBinaryExpression);
    }

    protected override Expression VisitSqlFunction(SqlFunctionExpression sqlFunctionExpression)
    {
        if (TryGetAggregateFunction(sqlFunctionExpression, out var aggregateFunction) && TryGetAggregateFunctionName(aggregateFunction.Name, out var kustoName))
        {
            Sql.Append(kustoName);
            Sql.Append("(");

            var arguments = aggregateFunction.Arguments.Where(argument => !IsStarFragment(argument)).ToList();
            for (var i = 0; i < arguments.Count; i++)
            {
                if (i > 0)
                    Sql.Append(", ");

                Visit(UnwrapConversions(arguments[i]));
            }

            Sql.Append(")");
            return sqlFunctionExpression;
        }

        return base.VisitSqlFunction(sqlFunctionExpression);
    }

    private void WriteProjection(SelectExpression select)
    {
        if (select.Projection.Count == 0)
            return;

        if (select.Projection.LastOrDefault().Expression is RowNumberExpression)
        {
            Sql.AppendLine();
            Sql.Append("| serialize ");
        }

        Sql.AppendLine();
        Sql.Append("| project ");

        var usedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < select.Projection.Count; i++)
        {
            var projection = select.Projection[i];

            if (i > 0)
                Sql.Append(", ");

            var alias = projection.Alias;
            if (!alias.IsNullOrEmpty())
            {
                alias = MakeUniqueAlias(alias, usedAliases);
                Sql.Append(alias + " = ");
            }

            if (projection.Expression is RowNumberExpression)
            {
                Sql.Append("row_number(0)");
            }
            else
            {
                Visit(projection.Expression);
            }

            if (projection.Expression is ExistsExpression)
            {
                Sql.Append(" | count | where Count > 0 | project 1");
            }
        }

        if (_outerApplyHandler.IsActive && _outerApplyHandler.JoinKeyColumns.Count > 0)
        {
            foreach (var joinKeyColumn in _outerApplyHandler.JoinKeyColumns)
            {
                var alreadyProjected = select.Projection.Any(projection =>
                    projection.Expression is ColumnExpression column &&
                    string.Equals(column.Name, joinKeyColumn, StringComparison.OrdinalIgnoreCase));

                if (alreadyProjected)
                    continue;

                Sql.Append(", ");
                Sql.Append(joinKeyColumn);
            }
        }
    }

    private void WriteOrderBy(SelectExpression select)
    {
        if (select.Orderings.Count == 0)
            return;

        Sql.AppendLine();
        Sql.Append("| order by ");

        for (var i = 0; i < select.Orderings.Count; i++)
        {
            if (i > 0)
                Sql.Append(", ");

            if (select.GroupBy.Count > 0 && TryWriteOrderedProjectionAlias(select, select.Orderings[i].Expression))
            {
                Sql.Append(select.Orderings[i].IsAscending ? " asc" : " desc");
                continue;
            }

            Visit(select.Orderings[i].Expression);
            Sql.Append(select.Orderings[i].IsAscending ? " asc" : " desc");
        }
    }

    private bool TryWriteOrderedProjectionAlias(SelectExpression select, SqlExpression expression)
    {
        var projection = select.Projection.FirstOrDefault(candidate =>
            !string.IsNullOrWhiteSpace(candidate.Alias) &&
            ExpressionsEquivalent(candidate.Expression, expression));

        if (projection.Alias == null)
            return false;

        Sql.Append(projection.Alias);
        return true;
    }

    private void WriteSkip(SelectExpression select)
    {
        if (select.Offset == null)
            return;

        Sql.Append(", skip_index = row_number(1)");
        Sql.AppendLine();
        Sql.Append("| where skip_index > ");
        Visit(select.Offset);
    }

    private void WriteTake(SelectExpression select)
    {
        if (select.Limit == null)
            return;

        if (_outerApplyHandler.IsActive)
        {
            _outerApplyHandler.WritePartitionTake(select.Limit, Sql, expr => Visit(expr));
            return;
        }

        Sql.AppendLine();
        Sql.Append("| take ");
        Visit(select.Limit);
    }

    protected override Expression VisitColumn(ColumnExpression column)
    {
        Sql.Append(column.Name);
        return column;
    }

    protected override Expression VisitSqlConstant(SqlConstantExpression constant)
    {
        if (constant.Value == null)
        {
            Sql.Append("null");
            return constant;
        }

        switch (Type.GetTypeCode(constant.Value.GetType()))
        {
            case TypeCode.String:
                Sql.Append($"\"{constant.Value}\"");
                break;

            case TypeCode.Boolean:
                Sql.Append((bool)constant.Value ? "true" : "false");
                break;

            default:
                Sql.Append(Convert.ToString(constant.Value, CultureInfo.InvariantCulture));
                break;
        }

        return constant;
    }

    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var name = sqlParameterExpression.Name;
        Sql.AddParameter(
            name,
            name,
            sqlParameterExpression.TypeMapping!,
            sqlParameterExpression.IsNullable);

        Sql.Append(name.Substring(2));
        return sqlParameterExpression;
    }

    protected override string GetOperator(SqlBinaryExpression binaryExpression)
    {
        return binaryExpression.OperatorType switch
        {
            ExpressionType.Equal => " == ",
            ExpressionType.And => " and ",
            ExpressionType.AndAlso => " and ",
            ExpressionType.OrElse => " or ",
            ExpressionType.Or => " or ",
            _ => base.GetOperator(binaryExpression)
        };
    }

    protected override void GenerateIn(InExpression inExpression, bool negated)
    {
        Visit(inExpression.Item);
        Sql.Append(negated ? " !in (" : " in (");

        for (var i = 0; i < inExpression.Values.Count; i++)
        {
            if (i > 0)
                Sql.Append(", ");

            Visit(inExpression.Values[i]);
        }

        Sql.Append(")");
    }

    private static bool TryGetAggregateFunctionName(string functionName, out string kustoName)
    {
        switch (functionName.ToLowerInvariant())
        {
            case "count":
            case "count_big":
                kustoName = "count";
                return true;
            case "sum":
                kustoName = "sum";
                return true;
            case "min":
                kustoName = "min";
                return true;
            case "max":
                kustoName = "max";
                return true;
            case "avg":
            case "average":
                kustoName = "avg";
                return true;
            default:
                kustoName = string.Empty;
                return false;
        }
    }

    private static bool IsAggregateExpression(SqlExpression expression)
        => TryGetAggregateFunction(expression, out _);

    private static bool TryGetAggregateFunction(SqlExpression expression, out SqlFunctionExpression function)
    {
        expression = UnwrapConversions(expression);

        if (expression is SqlFunctionExpression directFunction && TryGetAggregateFunctionName(directFunction.Name, out _))
        {
            function = directFunction;
            return true;
        }

        if (expression is SqlBinaryExpression { OperatorType: ExpressionType.Coalesce } coalesceBinary &&
            TryGetAggregateFunction(coalesceBinary.Left, out function))
        {
            return true;
        }

        if (expression is SqlFunctionExpression wrapperFunction &&
            wrapperFunction.Name.Equals("coalesce", StringComparison.OrdinalIgnoreCase) &&
            wrapperFunction.Arguments.Count > 0 &&
            TryGetAggregateFunction(wrapperFunction.Arguments[0], out function))
        {
            return true;
        }

        function = null!;
        return false;
    }

    private static SqlExpression UnwrapConversions(SqlExpression expression)
    {
        while (expression is SqlUnaryExpression unary && unary.OperatorType == ExpressionType.Convert)
        {
            expression = unary.Operand;
        }

        return expression;
    }

    private static bool IsStarFragment(SqlExpression expression)
        => expression is SqlFragmentExpression fragment && fragment.Sql == "*";

    private static bool ExpressionsEquivalent(SqlExpression left, SqlExpression right)
    {
        left = UnwrapConversions(left);
        right = UnwrapConversions(right);

        if (ReferenceEquals(left, right))
            return true;

        if (left is ColumnExpression leftColumn && right is ColumnExpression rightColumn)
        {
            return string.Equals(leftColumn.Name, rightColumn.Name, StringComparison.OrdinalIgnoreCase);
        }

        if (left is SqlFunctionExpression leftFunction && right is SqlFunctionExpression rightFunction)
        {
            if (!string.Equals(leftFunction.Name, rightFunction.Name, StringComparison.OrdinalIgnoreCase))
                return false;

            var leftArguments = leftFunction.Arguments.Where(argument => !IsStarFragment(argument)).ToList();
            var rightArguments = rightFunction.Arguments.Where(argument => !IsStarFragment(argument)).ToList();
            if (leftArguments.Count != rightArguments.Count)
                return false;

            for (var i = 0; i < leftArguments.Count; i++)
            {
                if (!ExpressionsEquivalent(leftArguments[i], rightArguments[i]))
                    return false;
            }

            return true;
        }

        return false;
    }

    private static string MakeUniqueAlias(string baseAlias, HashSet<string> usedAliases)
    {
        if (usedAliases.Add(baseAlias))
            return baseAlias;

        for (var i = 1;; i++)
        {
            var candidate = $"{baseAlias}_{i}";
            if (usedAliases.Add(candidate))
                return candidate;
        }
    }
}





