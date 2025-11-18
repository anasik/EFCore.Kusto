using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Kusto.Infrastructure.Internal;

public class KustoOptionsExtension : RelationalOptionsExtension
{
    protected override RelationalOptionsExtension Clone()
    {
        throw new NotImplementedException();
    }

    public override void ApplyServices(IServiceCollection services)
    {
        throw new NotImplementedException();
    }

    public override DbContextOptionsExtensionInfo Info { get; }
}