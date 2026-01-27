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
    private SqlExpression? _extractedCorrelationPredicate;
    private string? _partitionColumn;
    private SqlExpression? _partitionLimit;
    private List<(SqlExpression expr, bool isAscending)>? _partitionOrderings;

    // MAIN ENTRY
    protected override Expression VisitSelect(SelectExpression select)
    {
        bool isNested = _selectDepth > 0;
        _selectDepth++;

        if (isNested)
        {
            Sql.Append("(");
            Sql.AppendLine();
        }

        WriteFrom(select);
        WriteWhere(select);
        WriteOrderBy(select);
        WriteProjection(select);
        WriteSkip(select);
        WriteTake(select);

        if (isNested)
        {
            Sql.AppendLine();
            Sql.Append(")");
        }

        _selectDepth--;
        return select;
    }

    // ============================================================
    // FROM clause
    // ============================================================

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

            case OuterApplyExpression outerApply:
                // For OUTER APPLY, unwrap to the inner table
                WriteSingleFrom(outerApply.Table);
                break;

            case CrossApplyExpression crossApply:
                // For CROSS APPLY, unwrap to the inner table
                WriteSingleFrom(crossApply.Table);
                break;

            default:
                throw new NotSupportedException($"Unsupported table expression: {table.GetType().Name}");
        }
    }

    private void WriteJoinedFrom(SelectExpression select)
    {
        // LEFT side (first table)
        var left = select.Tables[0];
        WriteSingleFrom(left);

        // assume all additional tables are joined in order (EF patterns)
        for (int i = 1; i < select.Tables.Count; i++)
        {
            var right = select.Tables[i];

            Sql.AppendLine();
            Sql.Append("| join kind=leftouter (");

            if (right is LeftJoinExpression leftJoin)
            {
                WriteSingleFrom(right);
                Sql.Append(") on ");
                WriteJoinPredicate(leftJoin.JoinPredicate);
            }
            else if (right is OuterApplyExpression outerApply)
            {
                // For OUTER APPLY, use Kusto's partition hint with native strategy
                // This gives us exactly N records per grouped entity
                var (predicate, selectWithCorrelation) = ExtractCorrelationPredicateAndSelect(outerApply.Table);
                if (predicate != null && predicate is SqlBinaryExpression binPred)
                {
                    // Extract partition key (right side of correlation - the grouping column)
                    if (binPred.Right is ColumnExpression partitionCol)
                    {
                        _partitionColumn = partitionCol.Name;

                        // Extract limit and orderings from the SELECT that contains the correlation predicate
                        if (selectWithCorrelation != null)
                        {
                            _partitionLimit = selectWithCorrelation.Limit;
                            _partitionOrderings = new List<(SqlExpression, bool)>(selectWithCorrelation.Orderings.Select(o => (o.Expression, o.IsAscending)));
                            _extractedCorrelationPredicate = predicate;
                        }

                        WriteSingleFrom(right);

                        _partitionColumn = null;
                        _partitionLimit = null;
                        _partitionOrderings = null;
                        _extractedCorrelationPredicate = null;
                    }
                    else
                    {
                        throw new NotSupportedException($"Correlation predicate right side must be a column");
                    }

                    Sql.Append(") on ");
                    WriteJoinPredicate(predicate);
                }
                else
                {
                    throw new NotSupportedException($"Could not extract correlation predicate from OUTER APPLY");
                }
            }
            else if (right is CrossApplyExpression crossApply)
            {
                // For CROSS APPLY converted to LEFT JOIN, find the join predicate
                var predicate = ExtractCorrelationPredicate(crossApply.Table);
                if (predicate != null)
                {
                    // Set flag so WriteWhere knows to skip this predicate
                    _extractedCorrelationPredicate = predicate;
                    WriteSingleFrom(right);
                    _extractedCorrelationPredicate = null;

                    Sql.Append(") on ");
                    WriteJoinPredicate(predicate);
                }
                else
                {
                    throw new NotSupportedException($"Could not extract correlation predicate from CROSS APPLY");
                }
            }
            else
            {
                throw new NotSupportedException($"Unsupported join expression: {right.GetType().Name}");
            }
        }
    }

    private void WriteJoinPredicate(SqlExpression predicate)
    {
        if (predicate is SqlBinaryExpression b && b.OperatorType == ExpressionType.Equal)
        {
            // left
            WriteJoinSide(b.Left, isLeft: true);

            Sql.Append(" == ");

            // right
            WriteJoinSide(b.Right, isLeft: false);
        }
        else
        {
            throw new NotSupportedException("Unsupported join predicate");
        }
    }

    private void WriteJoinSide(SqlExpression expr, bool isLeft)
    {
        // EF uses ColumnExpression for join keys
        if (expr is ColumnExpression c)
        {
            Sql.Append(isLeft ? "$left." : "$right.");
            Sql.Append(c.Name);
            return;
        }

        throw new NotSupportedException($"Unsupported join key expression: {expr.GetType().Name}");
    }

    /// <summary>
    /// Extracts a correlation predicate from an APPLY expression's inner table.
    /// Simply finds the first column-to-column equality predicate without removing it.
    /// The predicate stays in the WHERE clause to ensure correct filtering.
    /// </summary>
    private SqlExpression? ExtractCorrelationPredicate(TableExpressionBase applyInner)
    {
        if (applyInner is not SelectExpression select)
            return null;

        return FindCorrelationInSelect(select);
    }

    /// <summary>
    /// Extracts a correlation predicate from an APPLY expression and returns both
    /// the predicate and the SelectExpression containing it (for correct LIMIT extraction).
    /// </summary>
    private (SqlExpression? predicate, SelectExpression? selectWithPredicate) ExtractCorrelationPredicateAndSelect(TableExpressionBase applyInner)
    {
        if (applyInner is not SelectExpression select)
            return (null, null);

        return FindCorrelationInSelectWithContext(select);
    }

    /// <summary>
    /// Recursively searches for a correlation predicate (column == column) in a SELECT's WHERE clause
    /// or in nested SELECT expressions within its FROM clause.
    /// </summary>
    private SqlExpression? FindCorrelationInSelect(SelectExpression select)
    {
        if (select.Predicate != null)
        {
            // Try to find correlation predicate in this WHERE clause
            var found = FindCorrelationInExpression(select.Predicate);
            if (found != null)
                return found;
        }

        // Not found at this level, search nested SELECTs in FROM clause
        foreach (var table in select.Tables)
        {
            if (table is SelectExpression nested)
            {
                var found = FindCorrelationInSelect(nested);
                if (found != null)
                    return found;
            }
        }

        return null;
    }

    /// <summary>
    /// Recursively searches for a correlation predicate and returns both the predicate
    /// and the SelectExpression that contains it (for extracting LIMIT/ORDERINGS).
    /// </summary>
    private (SqlExpression? predicate, SelectExpression? selectWithPredicate) FindCorrelationInSelectWithContext(SelectExpression select)
    {
        if (select.Predicate != null)
        {
            // Try to find correlation predicate in this WHERE clause
            var found = FindCorrelationInExpression(select.Predicate);
            if (found != null)
                return (found, select); // Return THIS select since it has the predicate
        }

        // Not found at this level, search nested SELECTs in FROM clause
        foreach (var table in select.Tables)
        {
            if (table is SelectExpression nested)
            {
                var (predicate, selectWithPred) = FindCorrelationInSelectWithContext(nested);
                if (predicate != null)
                    return (predicate, selectWithPred); // Return the nested select that has it
            }
        }

        return (null, null);
    }

    /// <summary>
    /// Searches within an expression tree (which may be a complex AND/OR/NOT tree)
    /// to find a simple column-to-column equality predicate.
    /// </summary>
    private SqlExpression? FindCorrelationInExpression(SqlExpression expr)
    {
        if (expr is not SqlBinaryExpression binary)
            return null;

        // Check if THIS expression is a correlation predicate (col == col)
        if (binary.OperatorType == ExpressionType.Equal &&
            binary.Left is ColumnExpression &&
            binary.Right is ColumnExpression)
        {
            return binary;
        }

        // If this is AND/OR/NOT, recursively search both sides
        if (IsLogicalOperator(binary.OperatorType))
        {
            // Search left side
            var leftResult = FindCorrelationInExpression(binary.Left);
            if (leftResult != null)
                return leftResult;

            // Search right side
            var rightResult = FindCorrelationInExpression(binary.Right);
            if (rightResult != null)
                return rightResult;
        }

        return null;
    }

    /// <summary>
    /// Helper to check if an operator type is a logical operator (AND, OR, NOT).
    /// </summary>
    private static bool IsLogicalOperator(ExpressionType opType)
    {
        return opType == ExpressionType.And ||
               opType == ExpressionType.AndAlso ||
               opType == ExpressionType.Or ||
               opType == ExpressionType.OrElse ||
               opType == ExpressionType.Not;
    }

    // ============================================================
    // WHERE
    // ============================================================
    private void WriteWhere(SelectExpression select)
    {
        if (select.Predicate == null)
            return;

        var p = select.Predicate as SqlBinaryExpression;
        if (p != null)
        {
            var l = p.Left as ColumnExpression;
            if (l?.Name == "row" && p.OperatorType == ExpressionType.LessThanOrEqual)
                return;
        }

        // If we extracted a correlation predicate for a JOIN, remove it from the WHERE clause
        var predicateToRender = select.Predicate;
        if (_extractedCorrelationPredicate != null)
        {
            var cleaned = RemoveCorrelationFromPredicate(select.Predicate, _extractedCorrelationPredicate);
            if (cleaned == null)
            {
                // The correlation was the entire WHERE clause
                return;
            }
            predicateToRender = cleaned;
        }

        Sql.AppendLine();
        Sql.Append("| where ");
        Visit(predicateToRender);
    }

    /// <summary>
    /// Removes a specific correlation predicate from an expression tree.
    /// Returns null if the correlation was the entire WHERE clause, otherwise returns the remaining predicate.
    /// </summary>
    private SqlExpression? RemoveCorrelationFromPredicate(SqlExpression expr, SqlExpression correlation)
    {
        if (expr is not SqlBinaryExpression binary)
            return expr;

        // If this IS the correlation, return null (nothing remains)
        if (ExpressionEquals(binary, correlation))
        {
            return null;
        }

        // If this is AND, try to extract the correlation from one side
        if (binary.OperatorType == ExpressionType.And || binary.OperatorType == ExpressionType.AndAlso)
        {
            if (ExpressionEquals(binary.Left, correlation))
            {
                // Correlation is on left, return right
                return binary.Right;
            }

            if (ExpressionEquals(binary.Right, correlation))
            {
                // Correlation is on right, return left
                return binary.Left;
            }

            // Correlation might be nested deeper, recurse
            var cleanLeft = RemoveCorrelationFromPredicate(binary.Left, correlation);
            var cleanRight = RemoveCorrelationFromPredicate(binary.Right, correlation);

            if (cleanLeft == null)
            {
                return cleanRight ?? binary.Right;
            }
            if (cleanRight == null)
            {
                return cleanLeft ?? binary.Left;
            }

            if (cleanLeft != binary.Left || cleanRight != binary.Right)
            {
                // Something changed, but we can't easily reconstruct the AND tree
                // So return the original - it will still work, just not perfectly optimized
                return expr;
            }
        }

        return expr;
    }

    /// <summary>
    /// Simple expression equality check for predicates.
    /// </summary>
    private static bool ExpressionEquals(SqlExpression expr1, SqlExpression expr2)
    {
        if (expr1 is SqlBinaryExpression b1 && expr2 is SqlBinaryExpression b2)
        {
            return b1.OperatorType == b2.OperatorType &&
                   ExpressionEquals(b1.Left, b2.Left) &&
                   ExpressionEquals(b1.Right, b2.Right);
        }

        if (expr1 is ColumnExpression c1 && expr2 is ColumnExpression c2)
        {
            return c1.Name == c2.Name;
        }

        return object.ReferenceEquals(expr1, expr2);
    }

    protected override Expression VisitSqlUnary(SqlUnaryExpression sqlUnaryExpression)
    {
        if (sqlUnaryExpression.OperatorType == ExpressionType.Equal)
        {
            Sql.Append($" isnull(");
            Visit(sqlUnaryExpression.Operand);
            Sql.Append($") ");
            return sqlUnaryExpression;
        }

        if (sqlUnaryExpression.OperatorType == ExpressionType.NotEqual)
        {
            Sql.Append($" isnotnull(");
            Visit(sqlUnaryExpression.Operand);
            Sql.Append($") ");
            return sqlUnaryExpression;
        }

        if(sqlUnaryExpression.OperatorType == ExpressionType.Not)
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
            string? op = operatorType switch
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

    // ============================================================
    // PROJECT
    // ============================================================
    private void WriteProjection(SelectExpression select)
    {
        if (select.Projection.Count == 0)
            return;

        bool rr = false;
        if (select.Projection.LastOrDefault().Expression is RowNumberExpression r)
        {
            rr = true;
            Sql.AppendLine();
            Sql.Append("| serialize ");
        }

        Sql.AppendLine();
        Sql.Append("| project ");

        var usedAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < select.Projection.Count; i++)
        {
            var proj = select.Projection[i];

            if (i > 0)
                Sql.Append(", ");

            var alias = proj.Alias;
            if (!alias.IsNullOrEmpty())
            {
                alias = MakeUniqueAlias(alias, usedAliases);
                Sql.Append(alias + " = ");
            }

            if (proj.Expression is RowNumberExpression)
                Sql.Append("row_number(0)");
            else
                Visit(proj.Expression);
            if (proj.Expression is ExistsExpression)
            {
                Sql.Append(" | count | where Count  > 0 | project 1");
            }
        }
    }

    // ============================================================
    // ORDER BY
    // ============================================================
    private void WriteOrderBy(SelectExpression select)
    {
        if (select.Orderings.Count == 0)
            return;

        Sql.AppendLine();
        Sql.Append("| order by ");

        for (int i = 0; i < select.Orderings.Count; i++)
        {
            if (i > 0)
                Sql.Append(", ");

            Visit(select.Orderings[i].Expression);
            Sql.Append(select.Orderings[i].IsAscending ? " asc" : " desc");
        }
    }

    // ============================================================
    // SKIP
    // ============================================================
    private void WriteSkip(SelectExpression select)
    {
        if (select.Offset == null)
            return;

        Sql.Append(", skip_index = row_number(1)");
        Sql.AppendLine();
        Sql.Append("| where skip_index > ");
        Visit(select.Offset);
    }

    // ============================================================
    // TAKE
    // ============================================================
    private void WriteTake(SelectExpression select)
    {
        if (select.Limit == null)
            return;

        // If we're in partition mode (OUTER APPLY), use partition hint for the inner limit
        if (_partitionColumn != null && _partitionLimit != null)
        {
            Sql.AppendLine();
            Sql.Append("| partition hint.strategy=native by ");
            Sql.Append(_partitionColumn);
            Sql.Append(" (");

            // Add the ordering if present
            if (_partitionOrderings?.Count > 0)
            {
                Sql.Append("top ");
                Visit(select.Limit);
                Sql.Append(" by ");

                for (int i = 0; i < _partitionOrderings.Count; i++)
                {
                    if (i > 0)
                        Sql.Append(", ");
                    Visit(_partitionOrderings[i].expr);
                    Sql.Append(_partitionOrderings[i].isAscending ? " asc" : " desc");
                }
            }
            else
            {
                Sql.Append("top ");
                Visit(select.Limit);
            }

            Sql.Append(")");

            return;
        }

        Sql.AppendLine();
        Sql.Append("| take ");
        Visit(select.Limit);
    }

    private static string MakeUniqueAlias(string baseAlias, HashSet<string> usedAliases)
    {
        if (usedAliases.Add(baseAlias))
            return baseAlias;

        for (int i = 1;; i++)
        {
            var candidate = $"{baseAlias}_{i}";
            if (usedAliases.Add(candidate))
                return candidate;
        }
    }

    // ============================================================
    // Column
    // ============================================================
    protected override Expression VisitColumn(ColumnExpression column)
    {
        Sql.Append(column.Name);
        return column;
    }

    // ============================================================
    // SqlConstant → Kusto literal
    // ============================================================
    protected override Expression VisitSqlConstant(SqlConstantExpression c)
    {
        if (c.Value == null)
        {
            Sql.Append("null");
            return c;
        }

        switch (Type.GetTypeCode(c.Value.GetType()))
        {
            case TypeCode.String:
                Sql.Append($"\"{c.Value}\"");
                break;

            case TypeCode.Boolean:
                Sql.Append((bool)c.Value ? "true" : "false");
                break;

            default:
                Sql.Append(Convert.ToString(c.Value, CultureInfo.InvariantCulture));
                break;
        }

        return c;
    }

    // ============================================================
    // SqlParameter → substitute using cache
    // ============================================================
    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var name = sqlParameterExpression.Name;
        Sql.AddParameter(
            name,
            name,
            sqlParameterExpression.TypeMapping!,
            sqlParameterExpression.IsNullable);

        Sql.Append(name.Substring(2)); // remove leading __
        return sqlParameterExpression;
    }

    protected override string GetOperator(SqlBinaryExpression binaryExpression)
    {
        switch (binaryExpression.OperatorType)
        {
            case ExpressionType.Equal:
                return " == ";
            case ExpressionType.And:
            case ExpressionType.AndAlso:
                return " and ";
            case ExpressionType.OrElse:
            case ExpressionType.Or:
                return " or ";
        }

        return base.GetOperator(binaryExpression);
    }

    protected override void GenerateIn(InExpression inExpression, bool negated)
    {
        Visit(inExpression.Item);
        Sql.Append(negated ? " !in (" : " in (");

        for (int i = 0; i < inExpression.Values.Count; i++)
        {
            if (i > 0)
                Sql.Append(", ");

            Visit(inExpression.Values[i]);
        }

        Sql.Append(")");
    }

    private static string ToKustoLiteral(object? value)
    {
        return value switch
        {
            null => "null",
            string s => $"'{s.Replace("'", "''")}'",
            bool b => b ? "true" : "false",
            int i => i.ToString(),
            long l => l.ToString(),
            double d => d.ToString(CultureInfo.InvariantCulture),
            decimal m => m.ToString(CultureInfo.InvariantCulture),
            Guid g => $"'{g}'",
            DateTime dt => $"datetime({dt:O})",
            DateOnly dOnly => $"date({dOnly:yyyy-MM-dd})",
            TimeOnly tOnly => $"time({tOnly:HH:mm:ss.fffffff})",
            _ => $"'{value}'"
        };
    }
}