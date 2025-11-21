using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Metadata.Conventions.Infrastructure;

namespace EFCore.Kusto.Metadata.Conventions;

public sealed class KustoConventionSetBuilder(
    ProviderConventionSetBuilderDependencies dependencies,
    RelationalConventionSetBuilderDependencies relationalDependencies)
    : RelationalConventionSetBuilder(dependencies, relationalDependencies)
{
    public override ConventionSet CreateConventionSet()
    {
        var set = base.CreateConventionSet();

        set.Remove(typeof(ForeignKeyIndexConvention));
        set.Remove(typeof(ValueGenerationConvention));
        set.Remove(typeof(SequenceUniquificationConvention));
        set.Remove(typeof(RelationalValueGenerationConvention));

        return set;
    }
}