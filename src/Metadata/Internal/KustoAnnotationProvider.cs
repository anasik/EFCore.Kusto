using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace EFCore.Kusto.Metadata.Internal;

public sealed class KustoAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
    : RelationalAnnotationProvider(dependencies)
{
    // Kusto has no sequences, so return nothing.
    public override IEnumerable<IAnnotation> For(ISequence sequence, bool designTime)
        => Array.Empty<IAnnotation>();

    // Kusto has no identity columns, no value generation, return nothing.
    public override IEnumerable<IAnnotation> For(IColumn column, bool designTime)
        => Array.Empty<IAnnotation>();

    // Kusto has no table-specific annotations.
    public override IEnumerable<IAnnotation> For(ITable table, bool designTime)
        => Array.Empty<IAnnotation>();
}