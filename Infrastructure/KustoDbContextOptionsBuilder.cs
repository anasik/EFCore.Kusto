using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Kusto.Infrastructure;

public class KustoDbContextOptionsBuilder : RelationalDbContextOptionsBuilder<KustoDbContextOptionsBuilder,
    KustoOptionsExtension>
{
    public KustoDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder) : base(optionsBuilder)
    {
    }
}