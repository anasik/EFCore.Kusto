using System.Globalization;
using System.Linq.Expressions;
using Kusto.Cloud.Platform.Utils;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Query;

public sealed class KustoQuerySqlGenerator(QuerySqlGeneratorDependencies deps) : QuerySqlGenerator(deps)
{
    private int _selectDepth;
    private readonly OuterApplyPartitionHandler _outerApplyHandler = new();
    private static readonly Dictionary<string, string> _sqlToKqlAggregate = new(StringComparer.Ordinal)
    {
        ["MAX"] = "max",
        ["MIN"] = "min",
        ["SUM"] = "sum",
        ["AVG"] = "avg",
        ["AVERAGE"] = "avg",
        ["COUNT"] = "count",
        ["LONGCOUNT"] = "count",
    };


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
        if (select.GroupBy.Count > 0)
            WriteSummarizeFromGroupBy(select);
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
    // SUMMARIZE — EF GroupBy SelectExpression path
    // ============================================================

    private void WriteSummarizeFromGroupBy(SelectExpression select)
    {
        Sql.AppendLine();
        Sql.Append("| summarize");

        bool first = true;
        foreach (var proj in select.Projection)
        {
            if (select.GroupBy.Any(k => ReferenceEquals(k, proj.Expression) || k.Equals(proj.Expression)))
                continue;

            Sql.Append(first ? " " : ", ");
            first = false;
            Sql.Append(AliasFor(proj));
            Sql.Append(" = ");
            Visit(proj.Expression);
        }

        if (select.GroupBy.Count > 0)
        {
            Sql.Append(" by ");
            for (int i = 0; i < select.GroupBy.Count; i++)
            {
                if (i > 0) Sql.Append(", ");
                Visit(select.GroupBy[i]);
            }
        }
    }

    /// <summary>
    /// Emits aggregate SqlFunctionExpressions in their KQL form. Recognises
    /// <c>MAX/MIN/SUM/AVG(arg)</c>, the <c>COALESCE(&lt;agg&gt;, default)</c>
    /// wrapper EF inserts for non-nullable results, and the three COUNT shapes
    /// (<c>COUNT(*)</c>, <c>COUNT(CASE WHEN p THEN 1 END)</c>,
    /// <c>COUNT(DISTINCT col)</c>). Non-aggregate function calls fall through
    /// to the base implementation.
    /// </summary>
    protected override Expression VisitSqlFunction(SqlFunctionExpression fn)
    {
        if (fn.Name == "COALESCE" && fn.Arguments?.Count > 0
                                  && fn.Arguments[0] is SqlFunctionExpression coalesced
                                  && _sqlToKqlAggregate.ContainsKey(coalesced.Name))
            return Visit(coalesced);

        if (!_sqlToKqlAggregate.TryGetValue(fn.Name, out var kql))
            return base.VisitSqlFunction(fn);

        if (kql == "count")
        {
            var arg = fn.Arguments?.Count == 1 ? fn.Arguments[0] : null;
            switch (arg)
            {
                case CaseExpression { Operand: null, WhenClauses: { Count: 1 } w } when IsLiteralOne(w[0].Result):
                    Sql.Append("countif(");
                    Visit(w[0].Test);
                    Sql.Append(")");
                    return fn;
                case DistinctExpression de:
                    Sql.Append("dcount(");
                    Visit(de.Operand);
                    Sql.Append(")");
                    return fn;
                default:
                    Sql.Append("count()");
                    return fn;
            }
        }

        Sql.Append(kql);
        Sql.Append("(");
        for (int i = 0; i < fn.Arguments!.Count; i++)
        {
            if (i > 0) Sql.Append(", ");
            Visit(fn.Arguments[i]);
        }
        Sql.Append(")");
        return fn;
    }

    /// <summary>
    /// True if the expression tree contains any aggregate function call —
    /// either directly, inside a wrapper like <c>COALESCE(SUM(x), 0)</c>, or
    /// composed with binary/unary/case nodes (e.g. <c>SUM(x) * 2</c>,
    /// <c>MAX(x) - MIN(x)</c>). Used by <c>WriteProjection</c> to decide
    /// whether a slot should emit just its alias (because the column was
    /// already produced on a preceding <c>| summarize</c> line) or the full
    /// <c>alias = expression</c> form.
    /// </summary>
    private static bool ContainsAggregate(SqlExpression expr) => expr switch
    {
        SqlFunctionExpression fn when _sqlToKqlAggregate.ContainsKey(fn.Name) => true,
        SqlFunctionExpression fn => fn.Arguments?.Any(ContainsAggregate) ?? false,
        SqlBinaryExpression b => ContainsAggregate(b.Left) || ContainsAggregate(b.Right),
        SqlUnaryExpression u => ContainsAggregate(u.Operand),
        CaseExpression c =>
            c.WhenClauses.Any(w => ContainsAggregate(w.Result))
            || (c.ElseResult != null && ContainsAggregate(c.ElseResult)),
        _ => false,
    };

    /// <summary>
    /// Alias for a projection slot on the <c>| summarize</c> line: EF's
    /// supplied alias, or a Kusto-style fallback synthesized from the
    /// projection's outer aggregate (e.g. <c>sum_Amount</c>).
    /// </summary>
    private static string AliasFor(ProjectionExpression proj)
    {
        if (!string.IsNullOrEmpty(proj.Alias)) return proj.Alias;
        return AliasHint(proj.Expression) ?? "Value";
    }

    /// <summary>
    /// Suggested column name for an unaliased aggregate projection, or null
    /// if the expression isn't an aggregate at its outer layer.
    /// </summary>
    private static string? AliasHint(SqlExpression expr)
    {
        if (expr is not SqlFunctionExpression fn) return null;
        if (fn.Name == "COALESCE" && fn.Arguments?.Count > 0)
            return AliasHint(fn.Arguments[0]);
        if (!_sqlToKqlAggregate.TryGetValue(fn.Name, out var kql)) return null;

        if (kql == "count" && fn.Arguments?.Count == 1)
        {
            if (fn.Arguments[0] is DistinctExpression { Operand: ColumnExpression dc })
                return "dcount_" + dc.Name;
            if (fn.Arguments[0] is CaseExpression) return "countif_";
        }

        var col = fn.Arguments?.Count > 0 && fn.Arguments[0] is ColumnExpression c ? c.Name : "";
        return kql + "_" + col;
    }

    private static bool TryFindAggregateProjectionAlias(
        SelectExpression select, SqlExpression ordering, out string? alias)
    {
        foreach (var proj in select.Projection)
        {
            if (AliasHint(proj.Expression) == null) continue;
            if (ReferenceEquals(proj.Expression, ordering) || proj.Expression.Equals(ordering))
            {
                alias = AliasFor(proj);
                return true;
            }
        }

        alias = null;
        return false;
    }

    private static bool IsLiteralOne(SqlExpression expr)
        => expr is SqlConstantExpression { Value: int i } && i == 1;

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
            else if (_outerApplyHandler.IsApplyJoin(right))
            {
                _outerApplyHandler.ProcessApplyJoin(right, Sql, WriteSingleFrom, WriteJoinPredicate);
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

        var predicateToRender = _outerApplyHandler.IsActive
            ? _outerApplyHandler.GetCleanedPredicate(select.Predicate)
            : select.Predicate;

        if (predicateToRender == null)
            return;

        Sql.AppendLine();
        Sql.Append("| where ");
        Visit(predicateToRender);
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

    /// <summary>
    /// Translates SQL <c>CASE WHEN ... THEN ... ELSE ... END</c> emissions to
    /// their Kusto equivalents. Two-way searched CASE (one when-clause + an
    /// else) renders as <c>iif(cond, then, else)</c>; everything else renders
    /// as <c>case(p1, r1, p2, r2, ..., default)</c>. Simple CASE
    /// (<c>CASE x WHEN v THEN r</c>) is rewritten to its searched equivalent
    /// (<c>x == v</c> as the predicate). Missing else defaults to a bare
    /// <c>null</c> literal.
    /// </summary>
    protected override Expression VisitCase(CaseExpression caseExpression)
    {
        bool simple = caseExpression.Operand != null;

        if (!simple
            && caseExpression.WhenClauses.Count == 1
            && caseExpression.ElseResult != null)
        {
            Sql.Append("iif(");
            Visit(caseExpression.WhenClauses[0].Test);
            Sql.Append(", ");
            Visit(caseExpression.WhenClauses[0].Result);
            Sql.Append(", ");
            Visit(caseExpression.ElseResult);
            Sql.Append(")");
            return caseExpression;
        }

        Sql.Append("case(");
        for (int i = 0; i < caseExpression.WhenClauses.Count; i++)
        {
            if (i > 0) Sql.Append(", ");
            var w = caseExpression.WhenClauses[i];

            // Simple CASE: rewrite each WHEN value into a searched-equivalent
            // equality (`Operand == Test`) by constructing the binary
            // expression and letting VisitSqlBinary handle it — that path
            // owns provider-specific concerns (string strcmp, null handling,
            // precedence/parens).
            if (simple)
            {
                Visit(new SqlBinaryExpression(
                    ExpressionType.Equal,
                    caseExpression.Operand!,
                    w.Test,
                    typeof(bool),
                    typeMapping: null));
            }
            else
            {
                Visit(w.Test);
            }

            Sql.Append(", ");
            Visit(w.Result);
        }
        Sql.Append(", ");
        if (caseExpression.ElseResult != null)
            Visit(caseExpression.ElseResult);
        else
            Sql.Append("null");
        Sql.Append(")");

        return caseExpression;
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

            // Aggregate-bearing slots were already emitted as `alias = func(...)`
            // on the preceding `| summarize` line; emit just the alias here.
            if (ContainsAggregate(proj.Expression))
            {
                Sql.Append(AliasFor(proj));
                continue;
            }

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

        if (_outerApplyHandler.IsActive && _outerApplyHandler.JoinKeyColumn != null)
        {
            bool alreadyProjected = select.Projection.Any(p =>
                p.Expression is ColumnExpression col && col.Name == _outerApplyHandler.JoinKeyColumn);

            if (!alreadyProjected)
            {
                Sql.Append(", ");
                Sql.Append(_outerApplyHandler.JoinKeyColumn);
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

            var ord = select.Orderings[i];

            // After a `| summarize`, EF still puts the raw aggregate
            // SqlFunctionExpression (e.g. COALESCE(SUM(...), 0)) in
            // select.Orderings — visiting it would emit the SQL-shaped
            // function literal. Map it back to the projection alias that
            // sits on the | summarize line. For the non-GroupBy branch no
            // projection is an aggregate so this lookup always returns
            // false and emission falls through to Visit unchanged.
            if (TryFindAggregateProjectionAlias(select, ord.Expression, out var alias))
                Sql.Append(alias);
            else
                Visit(ord.Expression);

            Sql.Append(ord.IsAscending ? " asc" : " desc");
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

        if (_outerApplyHandler.IsActive)
        {
            _outerApplyHandler.WritePartitionTake(select.Limit, Sql, expr => Visit(expr));
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
    // SqlParameter: add to parameters collection if not already present, then emit parameter name without EF's leading "__" prefix
    // ============================================================
    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var name = sqlParameterExpression.Name;
        var existing = Sql.Parameters.FirstOrDefault(d => d.InvariantName == name);

        if (existing == null)
        {
            Sql.AddParameter(
                name,
                name,
                sqlParameterExpression.TypeMapping!,
                sqlParameterExpression.IsNullable);
        }

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
}