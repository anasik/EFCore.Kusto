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
        public string? JoinKeyColumn { get; set; }  
        public List<(SqlExpression expr, bool isAscending)> Orderings { get; set; } = new();
        public SqlExpression? ExtractedCorrelationPredicate { get; set; }
        public Action? WriteOuterKeys { get; set; }
        public string? OuterKeyColumn { get; set; }
    }

    private readonly Stack<PartitionContext> _contextStack = new();
    private int _outerAliasCount;
    private Action? _pendingOuterKeys;

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
    /// Gets the join key column that must be projected in the inner SELECT (if in OUTER APPLY mode).
    /// </summary>
    public string? JoinKeyColumn =>
        _contextStack.Count > 0 ? _contextStack.Peek().JoinKeyColumn : null;

    /// <summary>
    /// Checks if this table expression is an OUTER APPLY or CROSS APPLY.
    /// </summary>
    public bool IsApplyJoin(TableExpressionBase right) =>
        right is OuterApplyExpression or CrossApplyExpression;

    /// <summary>
    /// Binds the outer rows rendered so far to a materialized name, so the APPLY's inner side can
    /// restrict itself to the outer keys (see <see cref="WriteOuterKeySemiJoin"/>). Must be called
    /// before the <c>| join</c> line of the APPLY.
    /// </summary>
    public void WriteOuterBinding(IRelationalCommandBuilder sql)
    {
        var alias = $"__apply{_outerAliasCount++}";
        sql.AppendLine();
        sql.Append($"| as hint.materialized=true {alias}");
        _pendingOuterKeys = () => sql.Append(alias);
    }

    /// <summary>
    /// Sets how the next APPLY's inner side gets the outer rows to restrict itself to, when they can be
    /// written again instead of bound (e.g. the filtered outer table).
    /// </summary>
    public void SetOuterKeys(Action writeOuterKeys) => _pendingOuterKeys = writeOuterKeys;

    /// <summary>
    /// Writes <c>| join kind=leftsemi (outer | project key) on ...</c> in the select whose WHERE held the
    /// correlation predicate. Removing the correlation alone leaves the inner side uncorrelated, so it
    /// would be sorted/partitioned over every key before the APPLY join discards all but the outer ones.
    /// </summary>
    public void WriteOuterKeySemiJoin(IRelationalCommandBuilder sql)
    {
        var context = _contextStack.Peek();
        if (context.WriteOuterKeys == null || context.OuterKeyColumn == null || context.JoinKeyColumn == null)
            return;

        sql.AppendLine();
        sql.Append("| join kind=leftsemi (");
        context.WriteOuterKeys();
        sql.Append($" | project {context.OuterKeyColumn}) on $left.{context.JoinKeyColumn} == $right.{context.OuterKeyColumn}");
    }

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
        if (right is OuterApplyExpression or CrossApplyExpression)
        {
            ProcessApply((JoinExpressionBase)right, sql, writeSingleFrom, writeJoinPredicate);
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

    private void ProcessApply(
        JoinExpressionBase apply,
        IRelationalCommandBuilder sql,
        Action<TableExpressionBase> writeSingleFrom,
        Action<SqlExpression> writeJoinPredicate)
    {
        var (predicate, selectWithCorrelation) = ExtractCorrelationPredicateAndSelect(apply.Table);
        if (predicate == null)
            throw new NotSupportedException("Could not extract correlation predicate from APPLY");

        if (predicate is not SqlBinaryExpression binPred)
            throw new NotSupportedException("Expected binary expression for correlation predicate");

        binPred = OrientCorrelation(binPred, apply.Table);

        if (binPred.Right is not ColumnExpression partitionCol)
            throw new NotSupportedException("Correlation predicate right side must be a column");

        string? joinKeyCol = null;
        if (binPred.Right is ColumnExpression innerCol)
            joinKeyCol = innerCol.Name;

        if (selectWithCorrelation != null)
        {
            var orderings = new List<(SqlExpression, bool)>(
                selectWithCorrelation.Orderings.Select(o => (o.Expression, o.IsAscending)));
            PushOuterApplyContext(
                partitionCol.Name,
                joinKeyCol,
                orderings,
                predicate,
                (binPred.Left as ColumnExpression)?.Name);
        }

        writeSingleFrom(apply);

        PopOuterApplyContext();

        sql.Append(") on ");
        writeJoinPredicate(binPred);
    }

    /// <summary>
    /// Returns the correlation predicate as <c>outer == inner</c>: the side whose column belongs to a
    /// table inside the APPLY is inner. EF builds navigation correlations outer-first, but a hand-written
    /// one (<c>Set&lt;Media&gt;().Where(m =&gt; m.FK == p.Key)</c>) keeps the user's operand order.
    /// </summary>
    private static SqlBinaryExpression OrientCorrelation(SqlBinaryExpression predicate, TableExpressionBase applyInner)
    {
        var innerAliases = new HashSet<string>();
        CollectAliases(applyInner, innerAliases);

        if (predicate.Left is ColumnExpression l && predicate.Right is ColumnExpression r
            && innerAliases.Contains(l.TableAlias) == innerAliases.Contains(r.TableAlias))
            throw new NotSupportedException(
                $"APPLY correlation '{l.Name} == {r.Name}' does not compare an outer column with an inner column.");

        return predicate.Left is ColumnExpression left && innerAliases.Contains(left.TableAlias)
               && predicate.Right is ColumnExpression right && !innerAliases.Contains(right.TableAlias)
            ? predicate.Update(right, left)
            : predicate;
    }

    private static void CollectAliases(TableExpressionBase table, HashSet<string> aliases)
    {
        if (table.Alias != null)
            aliases.Add(table.Alias);

        switch (table)
        {
            case SelectExpression select:
                foreach (var t in select.Tables)
                    CollectAliases(t, aliases);
                break;
            case JoinExpressionBase join:
                CollectAliases(join.Table, aliases);
                break;
        }
    }

    // ============================================================
    // PRIVATE: CORRELATION PREDICATE EXTRACTION
    // ============================================================

    private (SqlExpression? predicate, SelectExpression? selectWithPredicate) ExtractCorrelationPredicateAndSelect(TableExpressionBase applyInner)
    {
        if (applyInner is not SelectExpression select)
            return (null, null);

        return FindCorrelationInSelectWithContext(select);
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
        string? joinKeyColumn,
        List<(SqlExpression, bool)> partitionOrderings,
        SqlExpression correlationPredicate,
        string? outerKeyColumn)
    {
        _contextStack.Push(new PartitionContext
        {
            Column = partitionColumn,
            JoinKeyColumn = joinKeyColumn,
            Orderings = partitionOrderings,
            ExtractedCorrelationPredicate = correlationPredicate,
            WriteOuterKeys = _pendingOuterKeys,
            OuterKeyColumn = outerKeyColumn
        });
        _pendingOuterKeys = null;
    }

    private void PopOuterApplyContext()
    {
        if (_contextStack.Count > 0)
            _contextStack.Pop();
    }
}
