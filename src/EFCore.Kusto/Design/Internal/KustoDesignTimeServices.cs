using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.Extensions.DependencyInjection;

[assembly: DesignTimeProviderServices("EFCore.Kusto.Design.Internal.KustoDesignTimeServices")]

namespace EFCore.Kusto.Design.Internal;

/// <summary>
/// Registers the design-time services the EF Core tooling (<c>dotnet ef</c>) needs to
/// scaffold and script migrations for the Kusto provider. The Kusto provider emits no
/// custom relational annotations, so the base relational code generators suffice.
/// </summary>
public class KustoDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
    {
        serviceCollection.AddEntityFrameworkKusto();

        new EntityFrameworkRelationalDesignServicesBuilder(serviceCollection)
            .TryAdd<IAnnotationCodeGenerator, AnnotationCodeGenerator>()
            .TryAddCoreServices();
    }
}
