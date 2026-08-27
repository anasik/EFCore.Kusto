using System.Linq.Expressions;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Query.Internal;

/// <summary>
/// Represents one of Kusto's native case-sensitive string infix operators
/// (<c>contains_cs</c>, <c>startswith_cs</c>, <c>endswith_cs</c>) as a boolean
/// SQL expression, e.g. <c>(Operand contains_cs Pattern)</c>. These are
/// operators, not functions, in KQL, so they need their own expression node
/// rather than reusing <see cref="SqlFunctionExpression"/>.
/// </summary>
public sealed class KustoStringOperatorExpression : SqlExpression
{
    public KustoStringOperatorExpression(
        string kustoOperator,
        SqlExpression operand,
        SqlExpression pattern,
        RelationalTypeMapping? typeMapping)
        : base(typeof(bool), typeMapping)
    {
        KustoOperator = kustoOperator;
        Operand = operand;
        Pattern = pattern;
    }

    public string KustoOperator { get; }
    public SqlExpression Operand { get; }
    public SqlExpression Pattern { get; }

    protected override Expression VisitChildren(ExpressionVisitor visitor)
        => Update((SqlExpression)visitor.Visit(Operand), (SqlExpression)visitor.Visit(Pattern));

    public KustoStringOperatorExpression Update(SqlExpression operand, SqlExpression pattern)
        => operand == Operand && pattern == Pattern
            ? this
            : new KustoStringOperatorExpression(KustoOperator, operand, pattern, TypeMapping);

    protected override void Print(ExpressionPrinter expressionPrinter)
    {
        expressionPrinter.Append("(");
        expressionPrinter.Visit(Operand);
        expressionPrinter.Append($" {KustoOperator} ");
        expressionPrinter.Visit(Pattern);
        expressionPrinter.Append(")");
    }

    public override bool Equals(object? obj)
        => obj is KustoStringOperatorExpression other && Equals(other);

    private bool Equals(KustoStringOperatorExpression other)
        => base.Equals(other)
           && KustoOperator == other.KustoOperator
           && Operand.Equals(other.Operand)
           && Pattern.Equals(other.Pattern);

    public override int GetHashCode()
        => HashCode.Combine(base.GetHashCode(), KustoOperator, Operand, Pattern);

#if NET9_0_OR_GREATER
    private static ConstructorInfo? _quotingConstructor;

#pragma warning disable EF9100 // RelationalExpressionQuotingUtilities is evaluation-only; mirrors EF's own SqlBinaryExpression.Quote() implementation.
    public override Expression Quote()
        => Expression.New(
            _quotingConstructor ??= typeof(KustoStringOperatorExpression).GetConstructor(
                [typeof(string), typeof(SqlExpression), typeof(SqlExpression), typeof(RelationalTypeMapping)])!,
            Expression.Constant(KustoOperator),
            Operand.Quote(),
            Pattern.Quote(),
            RelationalExpressionQuotingUtilities.QuoteTypeMapping(TypeMapping));
#pragma warning restore EF9100
#endif
}
