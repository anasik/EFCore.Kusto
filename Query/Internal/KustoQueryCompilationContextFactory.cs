using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace EFCore.Kusto.Query.Internal;

public sealed class KustoQueryCompilationContextFactory
    : RelationalQueryCompilationContextFactory
{
    public KustoQueryCompilationContextFactory(
        QueryCompilationContextDependencies dependencies,
        RelationalQueryCompilationContextDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    public override QueryCompilationContext Create(bool async)
        => new KustoQueryCompilationContext(
            Dependencies,
            RelationalDependencies,
            async);
}