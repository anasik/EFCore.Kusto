using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;

namespace EFCore.Kusto.Query.Internal;

public class KustoQueryCompilationContextFactory : RelationalQueryCompilationContextFactory
{
    public KustoQueryCompilationContextFactory(QueryCompilationContextDependencies dependencies,
        RelationalQueryCompilationContextDependencies relationalDependencies) : base(dependencies,
        relationalDependencies)
    {
    }
}