using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Kusto.Infrastructure;

/// <summary>
/// Provides Kusto-specific configuration for <see cref="DbContextOptionsBuilder"/>.
/// </summary>
public class KustoDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    : RelationalDbContextOptionsBuilder<KustoDbContextOptionsBuilder,
        KustoOptionsExtension>(optionsBuilder)
{
    public virtual DbContextOptionsBuilder OptionsBuilder => base.OptionsBuilder;
}