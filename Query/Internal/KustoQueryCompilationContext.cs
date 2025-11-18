using Microsoft.EntityFrameworkCore.Query;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoQueryCompilationContext
    : RelationalQueryCompilationContext
{
    public KustoQueryCompilationContext(
        QueryCompilationContextDependencies dependencies,
        RelationalQueryCompilationContextDependencies relationalDependencies,
        bool async)
        : base(dependencies, relationalDependencies, async)
    {
    }
}