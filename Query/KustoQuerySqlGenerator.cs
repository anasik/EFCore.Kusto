using System.Globalization;
using System.Linq.Expressions;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Kusto.Query;

public sealed class KustoQuerySqlGenerator(QuerySqlGeneratorDependencies deps) : QuerySqlGenerator(deps)
{
    protected override Expression VisitSelect(SelectExpression select)
    {
        // ============================================================
        // 1. FROM (table source)
        // ============================================================
        if (select.Tables.Count == 0)
            throw new NotSupportedException("Kusto requires exactly one table.");

        var tableExpr = select.Tables[0];

        switch (tableExpr)
        {
            case TableExpression t:
                Sql.Append(t.Table.Name);
                break;

            case FromSqlExpression f:
                Sql.Append("(");
                Sql.Append(f.Sql);
                Sql.Append(")");
                break;

            default:
                throw new NotSupportedException(
                    $"Unsupported table expression: {tableExpr.GetType().Name}");
        }

        // ============================================================
        // 2. WHERE
        // ============================================================
        if (select.Predicate != null)
        {
            Sql.AppendLine();
            Sql.Append("| where ");
            Visit(select.Predicate);
        }

        // ============================================================
        // 3. PROJECTION (SELECT)
        // ============================================================
        if (select.Projection.Count > 0)
        {
            Sql.AppendLine();
            Sql.Append("| project ");

            for (int i = 0; i < select.Projection.Count; i++)
            {
                if (i > 0) Sql.Append(", ");

                var proj = select.Projection[i];

                if (proj.Alias != null)
                    Sql.Append($"{proj.Alias} = ");

                Visit(proj.Expression);
            }
        }

        // ============================================================
        // 4. ORDER BY
        // ============================================================
        if (select.Orderings.Count > 0)
        {
            Sql.AppendLine();
            Sql.Append("| order by ");

            for (int i = 0; i < select.Orderings.Count; i++)
            {
                if (i > 0) Sql.Append(", ");

                Visit(select.Orderings[i].Expression);
                Sql.Append(select.Orderings[i].IsAscending ? " asc" : " desc");
            }
        }

        // ============================================================
        // 5. SKIP
        // ============================================================
        if (select.Offset != null)
        {
            Sql.AppendLine();
            Sql.Append("| skip ");
            Visit(select.Offset);
        }

        // ============================================================
        // 6. TAKE
        // ============================================================
        if (select.Limit != null)
        {
            Sql.AppendLine();
            Sql.Append("| take ");
            Visit(select.Limit);
        }

        return select;
    }

    // ============================================================
    // Binary expressions → map to KQL operators
    // ============================================================
    protected override Expression VisitSqlBinary(SqlBinaryExpression b)
    {
        Sql.Append("(");

        Visit(b.Left);

        Sql.Append(b.OperatorType switch
        {
            ExpressionType.Equal => " == ",
            ExpressionType.NotEqual => " != ",
            ExpressionType.GreaterThan => " > ",
            ExpressionType.GreaterThanOrEqual => " >= ",
            ExpressionType.LessThan => " < ",
            ExpressionType.LessThanOrEqual => " <= ",
            ExpressionType.AndAlso => " and ",
            ExpressionType.OrElse => " or ",
            _ => throw new NotSupportedException($"Unsupported operator: {b.OperatorType}")
        });

        Visit(b.Right);

        Sql.Append(")");

        return b;
    }

    // ============================================================
    // Column expressions
    // ============================================================
    protected override Expression VisitColumn(ColumnExpression column)
    {
        Sql.Append(column.Name);
        return column;
    }

    // ============================================================
    // Constants → map to KQL literals
    // ============================================================
    protected override Expression VisitSqlConstant(SqlConstantExpression c)
    {
        if (c.Value == null)
        {
            Sql.Append("null");
            return c;
        }

        switch (c.Type.Name)
        {
            case "String":
                Sql.Append($"\"{c.Value}\"");
                break;

            case "Boolean":
                Sql.Append((bool)c.Value ? "true" : "false");
                break;

            default:
                Sql.Append((string)c.Value);
                break;
        }

        return c;
    }

    protected override Expression VisitParameter(ParameterExpression node)
    {
        return base.VisitParameter(node);
    }

    protected override Expression VisitSqlParameter(SqlParameterExpression sqlParameterExpression)
    {
        var name = sqlParameterExpression.Name;

        if (KustoValueCache.Values.TryGetValue(name, out var value))
        {
            // Emit correct literal directly to SQL builder
            Sql.Append(ToKustoLiteral(value));

            // DO NOT emit the sqlParameterExpression name
            return sqlParameterExpression;
        }

        // fallback (should never hit)
        Sql.Append(name);
        return sqlParameterExpression;
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