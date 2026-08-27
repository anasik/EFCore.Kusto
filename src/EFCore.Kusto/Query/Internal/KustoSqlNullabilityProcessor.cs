using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;

namespace EFCore.Kusto.Query.Internal;

/// <summary>
/// Teaches EF's null-semantics rewrite pass about <see cref="KustoStringOperatorExpression"/>.
/// The base <see cref="SqlNullabilityProcessor"/> throws on any custom
/// <see cref="SqlExpression"/> subtype it doesn't recognize, so every provider
/// introducing its own node type (as this one does for Kusto's native
/// contains_cs/startswith_cs/endswith_cs operators) has to override
/// <see cref="VisitCustomSqlExpression"/> for it.
/// </summary>
public sealed class KustoSqlNullabilityProcessor(
    RelationalParameterBasedSqlProcessorDependencies dependencies,
#if NET9_0_OR_GREATER
    // EF Core 9 replaced the plain bool useRelationalNulls constructor arg
    // with this parameters struct.
    RelationalParameterBasedSqlProcessorParameters parameters)
    : SqlNullabilityProcessor(dependencies, parameters)
#else
    bool useRelationalNulls)
    : SqlNullabilityProcessor(dependencies, useRelationalNulls)
#endif
{
    protected override SqlExpression VisitCustomSqlExpression(
        SqlExpression sqlExpression, bool allowOptimizedExpansion, out bool nullable)
    {
        if (sqlExpression is KustoStringOperatorExpression op)
        {
            var operand = Visit(op.Operand, out var operandNullable);
            var pattern = Visit(op.Pattern, out var patternNullable);
            nullable = operandNullable || patternNullable;
            return op.Update(operand, pattern);
        }

        return base.VisitCustomSqlExpression(sqlExpression, allowOptimizedExpansion, out nullable);
    }
}
