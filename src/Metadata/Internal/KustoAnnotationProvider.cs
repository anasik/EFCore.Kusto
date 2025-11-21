using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace EFCore.Kusto.Metadata.Internal;

/// <summary>
/// Supplies relational annotations for Kusto database artifacts.
/// </summary>
public sealed class KustoAnnotationProvider(RelationalAnnotationProviderDependencies dependencies)
    : RelationalAnnotationProvider(dependencies)
{
    /// <inheritdoc />
    public override IEnumerable<IAnnotation> For(ISequence sequence, bool designTime)
        => Array.Empty<IAnnotation>();

    /// <inheritdoc />
    public override IEnumerable<IAnnotation> For(IColumn column, bool designTime)
        => Array.Empty<IAnnotation>();

    /// <inheritdoc />
    public override IEnumerable<IAnnotation> For(ITable table, bool designTime)
        => Array.Empty<IAnnotation>();
}