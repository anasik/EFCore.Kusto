using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

public class KustoTypeMappingSource : RelationalTypeMappingSource
{
    public KustoTypeMappingSource(TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies) : base(dependencies, relationalDependencies)
    {
    }
}