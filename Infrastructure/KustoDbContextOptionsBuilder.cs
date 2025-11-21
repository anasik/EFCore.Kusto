using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EFCore.Kusto.Infrastructure;

public class KustoDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    : RelationalDbContextOptionsBuilder<KustoDbContextOptionsBuilder,
        KustoOptionsExtension>(optionsBuilder);