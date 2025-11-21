using System.Globalization;
using System.Linq.Expressions;
using EFCore.Kusto.Query.Internal;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Kusto.Query;

public sealed class KustoQuerySqlGenerator(QuerySqlGeneratorDependencies deps) : QuerySqlGenerator(deps)
{
    private int _selectDepth;

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
            WriteSingleFrom(right);
            Sql.Append(") on ");

            WriteJoinPredicate(((LeftJoinExpression)right).JoinPredicate);
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
        var l = p.Left as ColumnExpression;
        if (l?.Name == "row" && p.OperatorType == ExpressionType.LessThanOrEqual)
            return;

        Sql.AppendLine();
        Sql.Append("| where ");
        Visit(select.Predicate);
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

        return base.VisitSqlUnary(sqlUnaryExpression);
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

        for (int i = 0; i < select.Projection.Count; i++)
        {
            var proj = select.Projection[i];
            if (rr && proj.Alias == "RowVersion")
            {
                continue;
            }

            if (i > 0)
                Sql.Append(", ");

            if (proj.Alias != null)
                Sql.Append(proj.Alias + " = ");

            if (proj.Expression is RowNumberExpression)
                Sql.Append("row_number(0)");
            else
                Visit(proj.Expression);
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

        Sql.AppendLine();
        Sql.Append("| take ");
        Visit(select.Limit);
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

        if (KustoParameterCache.Values.TryGetValue(name, out var value))
        {
            Sql.Append(ToKustoLiteral(value));
            return sqlParameterExpression;
        }

        Sql.Append(name);
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