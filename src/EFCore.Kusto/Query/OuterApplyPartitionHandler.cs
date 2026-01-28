using System.Linq.Expressions;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Query;

/// <summary>
/// COMPLETELY encapsulates all OUTER APPLY and CROSS APPLY logic including:
/// - Correlation predicate extraction and manipulation
/// - Partition hint generation
/// - Join rendering
/// - WHERE clause predicate cleanup
///
/// The main generator has ZERO OUTER APPLY knowledge. All logic is here.
/// </summary>
internal class OuterApplyPartitionHandler
{
    private class PartitionContext
    {
        public string Column { get; set; } = string.Empty;
        public List<(SqlExpression expr, bool isAscending)> Orderings { get; set; } = new();
        public SqlExpression? ExtractedCorrelationPredicate { get; set; }
    }

    private readonly Stack<PartitionContext> _contextStack = new();

    /// <summary>
    /// Gets whether we're currently inside an OUTER/CROSS APPLY context.
    /// </summary>
    public bool IsActive => _contextStack.Count > 0;

    /// <summary>
    /// Gets the currently extracted correlation predicate (if any).
    /// </summary>
    public SqlExpression? ExtractedCorrelationPredicate =>
        _contextStack.Count > 0 ? _contextStack.Peek().ExtractedCorrelationPredicate : null;

    /// <summary>
    /// Checks if this table expression is an OUTER APPLY or CROSS APPLY.
    /// </summary>
    public bool IsApplyJoin(TableExpressionBase right) =>
        right is OuterApplyExpression or CrossApplyExpression;

    /// <summary>
    /// Completely handles OUTER APPLY or CROSS APPLY join rendering.
    /// Extracts correlation, manages partition context, renders the join.
    /// </summary>
    public void ProcessApplyJoin(
        TableExpressionBase right,
        IRelationalCommandBuilder sql,
        Action<TableExpressionBase> writeSingleFrom,
        Action<SqlExpression> writeJoinPredicate)
    {
        if (right is OuterApplyExpression outerApply)
        {
            ProcessOuterApply(outerApply, sql, writeSingleFrom, writeJoinPredicate);
        }
        else if (right is CrossApplyExpression crossApply)
        {
            ProcessCrossApply(crossApply, sql, writeSingleFrom, writeJoinPredicate);
        }
        else
        {
            throw new NotSupportedException($"Expected OUTER/CROSS APPLY, got {right.GetType().Name}");
        }
    }

    /// <summary>
    /// Cleans the WHERE clause by removing extracted correlation predicate.
    /// Returns null if predicate was the entire WHERE, otherwise returns cleaned predicate.
    /// </summary>
    public SqlExpression? GetCleanedPredicate(SqlExpression? predicate)
    {
        if (predicate == null || ExtractedCorrelationPredicate == null)
            return predicate;

        return RemoveCorrelationFromPredicate(predicate, ExtractedCorrelationPredicate);
    }

    /// <summary>
    /// Writes the partition hint and outer limit clauses if in OUTER APPLY mode.
    /// Generates: | partition hint.strategy=native by {column} (top {limit} by {orderings})
    ///            | take {outerLimit}
    /// </summary>
    public void WritePartitionTake(
        SqlExpression outerLimit,
        IRelationalCommandBuilder sql,
        Action<SqlExpression> visitExpression)
    {
        if (!IsActive)
            return;

        var context = _contextStack.Peek();

        // Only partition if we have a partition column (OUTER APPLY case)
        if (string.IsNullOrEmpty(context.Column))
            return;

        // Write partition hint with per-group limit
        sql.AppendLine();
        sql.Append("| partition hint.strategy=native by ");
        sql.Append(context.Column);
        sql.Append(" (");

        if (context.Orderings.Count > 0)
        {
            sql.Append("top ");
            visitExpression(outerLimit);
            sql.Append(" by ");

            for (int i = 0; i < context.Orderings.Count; i++)
            {
                if (i > 0)
                    sql.Append(", ");
                visitExpression(context.Orderings[i].expr);
                sql.Append(context.Orderings[i].isAscending ? " asc" : " desc");
            }
        }
        else
        {
            sql.Append("top ");
            visitExpression(outerLimit);
        }

        sql.Append(")");
    }

    // ============================================================
    // PRIVATE: OUTER APPLY / CROSS APPLY PROCESSING
    // ============================================================

    private void ProcessOuterApply(
        OuterApplyExpression outerApply,
        IRelationalCommandBuilder sql,
        Action<TableExpressionBase> writeSingleFrom,
        Action<SqlExpression> writeJoinPredicate)
    {
        var (predicate, selectWithCorrelation) = ExtractCorrelationPredicateAndSelect(outerApply.Table);
        if (predicate == null)
            throw new NotSupportedException("Could not extract correlation predicate from OUTER APPLY");

        if (predicate is not SqlBinaryExpression binPred)
            throw new NotSupportedException("Expected binary expression for correlation predicate");

        if (binPred.Right is not ColumnExpression partitionCol)
            throw new NotSupportedException("Correlation predicate right side must be a column");

        // Push OUTER APPLY context with partition info
        if (selectWithCorrelation != null)
        {
            var orderings = new List<(SqlExpression, bool)>(
                selectWithCorrelation.Orderings.Select(o => (o.Expression, o.IsAscending)));
            PushOuterApplyContext(
                partitionCol.Name,
                orderings,
                predicate);
        }

        writeSingleFrom(outerApply);

        PopOuterApplyContext();

        sql.Append(") on ");
        writeJoinPredicate(predicate);
    }

    private void ProcessCrossApply(
        CrossApplyExpression crossApply,
        IRelationalCommandBuilder sql,
        Action<TableExpressionBase> writeSingleFrom,
        Action<SqlExpression> writeJoinPredicate)
    {
        var predicate = ExtractCorrelationPredicate(crossApply.Table);
        if (predicate == null)
            throw new NotSupportedException("Could not extract correlation predicate from CROSS APPLY");

        // Push context to track extracted predicate (no partition for CROSS APPLY)
        PushOuterApplyContext(
            partitionColumn: "",
            partitionOrderings: new(),
            correlationPredicate: predicate);

        writeSingleFrom(crossApply);

        PopOuterApplyContext();

        sql.Append(") on ");
        writeJoinPredicate(predicate);
    }

    // ============================================================
    // PRIVATE: CORRELATION PREDICATE EXTRACTION
    // ============================================================

    private SqlExpression? ExtractCorrelationPredicate(TableExpressionBase applyInner)
    {
        if (applyInner is not SelectExpression select)
            return null;

        return FindCorrelationInSelect(select);
    }

    private (SqlExpression? predicate, SelectExpression? selectWithPredicate) ExtractCorrelationPredicateAndSelect(TableExpressionBase applyInner)
    {
        if (applyInner is not SelectExpression select)
            return (null, null);

        return FindCorrelationInSelectWithContext(select);
    }

    private SqlExpression? FindCorrelationInSelect(SelectExpression select)
    {
        if (select.Predicate != null)
        {
            var found = FindCorrelationInExpression(select.Predicate);
            if (found != null)
                return found;
        }

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

    private (SqlExpression? predicate, SelectExpression? selectWithPredicate) FindCorrelationInSelectWithContext(SelectExpression select)
    {
        if (select.Predicate != null)
        {
            var found = FindCorrelationInExpression(select.Predicate);
            if (found != null)
                return (found, select);
        }

        foreach (var table in select.Tables)
        {
            if (table is SelectExpression nested)
            {
                var (predicate, selectWithPred) = FindCorrelationInSelectWithContext(nested);
                if (predicate != null)
                    return (predicate, selectWithPred);
            }
        }

        return (null, null);
    }

    private SqlExpression? FindCorrelationInExpression(SqlExpression expr)
    {
        if (expr is not SqlBinaryExpression binary)
            return null;

        if (binary.OperatorType == ExpressionType.Equal &&
            binary.Left is ColumnExpression &&
            binary.Right is ColumnExpression)
        {
            return binary;
        }

        if (IsLogicalOperator(binary.OperatorType))
        {
            var leftResult = FindCorrelationInExpression(binary.Left);
            if (leftResult != null)
                return leftResult;

            var rightResult = FindCorrelationInExpression(binary.Right);
            if (rightResult != null)
                return rightResult;
        }

        return null;
    }

    private static bool IsLogicalOperator(ExpressionType opType) =>
        opType == ExpressionType.And ||
        opType == ExpressionType.AndAlso ||
        opType == ExpressionType.Or ||
        opType == ExpressionType.OrElse ||
        opType == ExpressionType.Not;

    // ============================================================
    // PRIVATE: PREDICATE MANIPULATION
    // ============================================================

    private SqlExpression? RemoveCorrelationFromPredicate(SqlExpression expr, SqlExpression correlation)
    {
        if (expr is not SqlBinaryExpression binary)
            return expr;

        if (ExpressionEquals(binary, correlation))
            return null;

        if (binary.OperatorType == ExpressionType.And || binary.OperatorType == ExpressionType.AndAlso)
        {
            if (ExpressionEquals(binary.Left, correlation))
                return binary.Right;

            if (ExpressionEquals(binary.Right, correlation))
                return binary.Left;

            var cleanLeft = RemoveCorrelationFromPredicate(binary.Left, correlation);
            var cleanRight = RemoveCorrelationFromPredicate(binary.Right, correlation);

            if (cleanLeft == null)
                return cleanRight ?? binary.Right;
            if (cleanRight == null)
                return cleanLeft ?? binary.Left;
            
            return binary.Update(cleanLeft, cleanRight);
        }

        return expr;
    }

    private static bool ExpressionEquals(SqlExpression expr1, SqlExpression expr2)
    {
        if (expr1 is SqlBinaryExpression b1 && expr2 is SqlBinaryExpression b2)
        {
            return b1.OperatorType == b2.OperatorType &&
                   ExpressionEquals(b1.Left, b2.Left) &&
                   ExpressionEquals(b1.Right, b2.Right);
        }

        if (expr1 is ColumnExpression c1 && expr2 is ColumnExpression c2)
            return c1.Name == c2.Name;

        return object.ReferenceEquals(expr1, expr2);
    }

    // ============================================================
    // PRIVATE: CONTEXT MANAGEMENT
    // ============================================================

    private void PushOuterApplyContext(
        string partitionColumn,
        List<(SqlExpression, bool)> partitionOrderings,
        SqlExpression correlationPredicate)
    {
        _contextStack.Push(new PartitionContext
        {
            Column = partitionColumn,
            Orderings = partitionOrderings,
            ExtractedCorrelationPredicate = correlationPredicate
        });
    }

    private void PopOuterApplyContext()
    {
        if (_contextStack.Count > 0)
            _contextStack.Pop();
    }
}
