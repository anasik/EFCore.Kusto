using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

public class KustoSqlGenerationHelper : RelationalSqlGenerationHelper
{
    public KustoSqlGenerationHelper(RelationalSqlGenerationHelperDependencies dependencies) : base(dependencies)
    {
    }
}