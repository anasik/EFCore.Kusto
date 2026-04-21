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
    private sealed class PartitionContext
    {
        public List<string> PartitionColumns { get; } = new();
        public List<string> JoinKeyColumns { get; } = new();
        public List<(SqlExpression expr, bool isAscending)> Orderings { get; set; } = new();
        public List<SqlExpression> ExtractedCorrelationPredicates { get; } = new();
    }

    private readonly Stack<PartitionContext> _contextStack = new();

    /// <summary>
    /// Gets whether we're currently inside an OUTER/CROSS APPLY context.
    /// </summary>
    public bool IsActive => _contextStack.Count > 0;

    /// <summary>
    /// Gets the join key columns that must be projected in the inner SELECT (if in APPLY mode).
    /// </summary>
    public IReadOnlyList<string> JoinKeyColumns =>
        _contextStack.Count > 0 ? _contextStack.Peek().JoinKeyColumns : Array.Empty<string>();

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
    /// Cleans the WHERE clause by removing extracted correlation predicates.
    /// Returns null if predicate was the entire WHERE, otherwise returns cleaned predicate.
    /// </summary>
    public SqlExpression? GetCleanedPredicate(SqlExpression? predicate)
    {
        if (predicate == null || _contextStack.Count == 0)
            return predicate;

        var cleaned = predicate;
        foreach (var correlation in _contextStack.Peek().ExtractedCorrelationPredicates)
        {
            cleaned = RemoveCorrelationFromPredicate(cleaned, correlation);
        }

        return cleaned;
    }

    /// <summary>
    /// Writes the partition hint and outer limit clauses if in OUTER APPLY mode.
    /// For composite keys, nested partitions are emitted to preserve the per-group semantics.
    /// </summary>
    public void WritePartitionTake(
        SqlExpression outerLimit,
        IRelationalCommandBuilder sql,
        Action<SqlExpression> visitExpression)
    {
        if (!IsActive)
            return;

        var context = _contextStack.Peek();
        if (context.PartitionColumns.Count == 0)
            return;

        sql.AppendLine();

        for (var i = 0; i < context.PartitionColumns.Count; i++)
        {
            sql.Append("| partition hint.strategy=native by ");
            sql.Append(context.PartitionColumns[i]);
            sql.Append(" (");
            sql.AppendLine();
        }

        sql.Append("top ");
        visitExpression(outerLimit);

        if (context.Orderings.Count > 0)
        {
            sql.Append(" by ");
            for (var i = 0; i < context.Orderings.Count; i++)
            {
                if (i > 0)
                    sql.Append(", ");

                visitExpression(context.Orderings[i].expr);
                sql.Append(context.Orderings[i].isAscending ? " asc" : " desc");
            }
        }

        for (var i = 0; i < context.PartitionColumns.Count; i++)
        {
            sql.Append(")");
        }
    }

    private void ProcessOuterApply(
        OuterApplyExpression outerApply,
        IRelationalCommandBuilder sql,
        Action<TableExpressionBase> writeSingleFrom,
        Action<SqlExpression> writeJoinPredicate)
    {
        var (predicates, selectWithCorrelation) = ExtractCorrelationPredicatesAndSelect(outerApply.Table);
        if (predicates.Count == 0)
            throw new NotSupportedException("Could not extract correlation predicate from OUTER APPLY");

        var partitionColumns = new List<string>();
        var joinKeyColumns = new List<string>();
        foreach (var predicate in predicates)
        {
            var innerColumn = TryGetInnerCorrelationColumn(predicate);
            if (innerColumn == null)
                throw new NotSupportedException("Correlation predicate must compare outer and inner columns.");

            partitionColumns.Add(innerColumn.Name);
            joinKeyColumns.Add(innerColumn.Name);
        }

        if (selectWithCorrelation != null)
        {
            var orderings = new List<(SqlExpression, bool)>(
                selectWithCorrelation.Orderings.Select(o => (o.Expression, o.IsAscending)));
            PushOuterApplyContext(partitionColumns, joinKeyColumns, orderings, predicates);
        }

        writeSingleFrom(outerApply);

        PopOuterApplyContext();

        sql.Append(") on ");
        writeJoinPredicate(CombinePredicates(predicates)!);
    }

    private void ProcessCrossApply(
        CrossApplyExpression crossApply,
        IRelationalCommandBuilder sql,
        Action<TableExpressionBase> writeSingleFrom,
        Action<SqlExpression> writeJoinPredicate)
    {
        var predicates = ExtractCorrelationPredicates(crossApply.Table);
        if (predicates.Count == 0)
            throw new NotSupportedException("Could not extract correlation predicate from CROSS APPLY");

        var joinKeyColumns = predicates
            .Select(TryGetInnerCorrelationColumn)
            .Where(column => column != null)
            .Select(column => column!.Name)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        PushOuterApplyContext(
            partitionColumns: Array.Empty<string>(),
            joinKeyColumns,
            partitionOrderings: new(),
            correlationPredicates: predicates);

        writeSingleFrom(crossApply);

        PopOuterApplyContext();

        sql.Append(") on ");
        writeJoinPredicate(CombinePredicates(predicates)!);
    }

    private static ColumnExpression? TryGetInnerCorrelationColumn(SqlExpression predicate)
    {
        if (predicate is not SqlBinaryExpression binary || binary.OperatorType != ExpressionType.Equal)
            return null;

        return binary.Right as ColumnExpression ?? binary.Left as ColumnExpression;
    }

    private List<SqlExpression> ExtractCorrelationPredicates(TableExpressionBase applyInner)
    {
        if (applyInner is not SelectExpression select)
            return new List<SqlExpression>();

        return FindCorrelationPredicatesInSelect(select);
    }

    private (List<SqlExpression> predicates, SelectExpression? selectWithPredicate) ExtractCorrelationPredicatesAndSelect(TableExpressionBase applyInner)
    {
        if (applyInner is not SelectExpression select)
            return (new List<SqlExpression>(), null);

        return FindCorrelationPredicatesInSelectWithContext(select);
    }

    private List<SqlExpression> FindCorrelationPredicatesInSelect(SelectExpression select)
    {
        if (select.Predicate != null)
        {
            var found = FindCorrelationPredicatesInExpression(select.Predicate);
            if (found.Count > 0)
                return found;
        }

        foreach (var table in select.Tables)
        {
            if (table is SelectExpression nested)
            {
                var found = FindCorrelationPredicatesInSelect(nested);
                if (found.Count > 0)
                    return found;
            }
        }

        return new List<SqlExpression>();
    }

    private (List<SqlExpression> predicates, SelectExpression? selectWithPredicate) FindCorrelationPredicatesInSelectWithContext(SelectExpression select)
    {
        if (select.Predicate != null)
        {
            var found = FindCorrelationPredicatesInExpression(select.Predicate);
            if (found.Count > 0)
                return (found, select);
        }

        foreach (var table in select.Tables)
        {
            if (table is SelectExpression nested)
            {
                var (predicates, selectWithPredicate) = FindCorrelationPredicatesInSelectWithContext(nested);
                if (predicates.Count > 0)
                    return (predicates, selectWithPredicate);
            }
        }

        return (new List<SqlExpression>(), null);
    }

    private List<SqlExpression> FindCorrelationPredicatesInExpression(SqlExpression expr)
    {
        var predicates = new List<SqlExpression>();
        CollectCorrelationPredicates(expr, predicates);
        return predicates;
    }

    private void CollectCorrelationPredicates(SqlExpression expr, List<SqlExpression> predicates)
    {
        if (expr is not SqlBinaryExpression binary)
            return;

        if (binary.OperatorType == ExpressionType.Equal &&
            binary.Left is ColumnExpression &&
            binary.Right is ColumnExpression)
        {
            predicates.Add(binary);
            return;
        }

        if (IsLogicalOperator(binary.OperatorType))
        {
            CollectCorrelationPredicates(binary.Left, predicates);
            CollectCorrelationPredicates(binary.Right, predicates);
        }
    }

    private static bool IsLogicalOperator(ExpressionType opType) =>
        opType == ExpressionType.And ||
        opType == ExpressionType.AndAlso ||
        opType == ExpressionType.Or ||
        opType == ExpressionType.OrElse ||
        opType == ExpressionType.Not;

    private SqlExpression? RemoveCorrelationFromPredicate(SqlExpression? expr, SqlExpression correlation)
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

        return ReferenceEquals(expr1, expr2);
    }

    private void PushOuterApplyContext(
        IEnumerable<string> partitionColumns,
        IEnumerable<string> joinKeyColumns,
        List<(SqlExpression, bool)> partitionOrderings,
        IEnumerable<SqlExpression> correlationPredicates)
    {
        var context = new PartitionContext
        {
            Orderings = partitionOrderings
        };

        context.PartitionColumns.AddRange(partitionColumns.Where(column => !string.IsNullOrWhiteSpace(column)));
        context.JoinKeyColumns.AddRange(joinKeyColumns.Where(column => !string.IsNullOrWhiteSpace(column)).Distinct(StringComparer.OrdinalIgnoreCase));
        context.ExtractedCorrelationPredicates.AddRange(correlationPredicates);

        _contextStack.Push(context);
    }

    private void PopOuterApplyContext()
    {
        if (_contextStack.Count > 0)
            _contextStack.Pop();
    }

    private static SqlExpression? CombinePredicates(IReadOnlyList<SqlExpression> predicates)
    {
        if (predicates.Count == 0)
            return null;

        var combined = predicates[0];
        for (var i = 1; i < predicates.Count; i++)
        {
            combined = new SqlBinaryExpression(
                ExpressionType.AndAlso,
                combined,
                predicates[i],
                combined.Type,
                combined.TypeMapping ?? predicates[i].TypeMapping);
        }

        return combined;
    }
}
