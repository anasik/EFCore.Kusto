using System;
using Azure.Identity;
using EFCore.Kusto.Extensions;
using EFCore.Kusto.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EFCore.Kusto.Tests;

public class KustoOptionsExtensionTests
{
    private const string Cluster = "https://example.westus.kusto.windows.net";
    private const string Database = "SampleDb";

    [Fact]
    public void UseKusto_sets_cluster_and_database()
    {
        var builder = new DbContextOptionsBuilder<TestContext>();

        builder.UseKusto(Cluster, Database);

        var extension = builder.Options.FindExtension<KustoOptionsExtension>();

        Assert.NotNull(extension);
        Assert.Equal(Cluster, extension!.ClusterUrl);
        Assert.Equal(Database, extension.Database);
        Assert.Equal(KustoAuthenticationStrategy.DefaultAzureCredential, extension.AuthenticationStrategy);
    }

    [Theory]
    [InlineData(null, Database, "clusterUrl")]
    [InlineData(" ", Database, "clusterUrl")]
    [InlineData(Cluster, null, "database")]
    [InlineData(Cluster, " ", "database")]
    public void UseKusto_validates_required_arguments(string? cluster, string? database, string expectedParam)
    {
        var builder = new DbContextOptionsBuilder<TestContext>();

        var ex = Assert.Throws<ArgumentException>(() => builder.UseKusto(cluster!, database!));

        Assert.Equal(expectedParam, ex.ParamName);
    }

    [Fact]
    public void UseKusto_configures_managed_identity()
    {
        var builder = new DbContextOptionsBuilder<TestContext>();

        builder.UseKusto(Cluster, Database, kusto => kusto.UseManagedIdentity("client-id"));

        var extension = builder.Options.FindExtension<KustoOptionsExtension>();

        Assert.NotNull(extension);
        Assert.Equal(KustoAuthenticationStrategy.ManagedIdentity, extension!.AuthenticationStrategy);
        Assert.Equal("client-id", extension.ManagedIdentityClientId);
        Assert.Null(extension.ApplicationClientId);
    }

    [Fact]
    public void UseKusto_configures_application_registration()
    {
        var builder = new DbContextOptionsBuilder<TestContext>();

        builder.UseKusto(Cluster, Database, kusto =>
            kusto.UseApplicationAuthentication("tenant-id", "client-id", "secret"));

        var extension = builder.Options.FindExtension<KustoOptionsExtension>();

        Assert.NotNull(extension);
        Assert.Equal(KustoAuthenticationStrategy.Application, extension!.AuthenticationStrategy);
        Assert.Equal("tenant-id", extension.ApplicationTenantId);
        Assert.Equal("client-id", extension.ApplicationClientId);
        Assert.Equal("secret", extension.ApplicationClientSecret);
    }

    [Fact]
    public void AddEntityFrameworkKusto_registers_provider_services()
    {
        var services = new ServiceCollection();

        services.AddEntityFrameworkKusto();

        using var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetService<IDatabaseProvider>());
    }

    [Fact]
    public void Service_registration_adds_credentials()
    {
        var services = new ServiceCollection();

        services.AddKustoManagedIdentityCredential("client-id");
        services.AddKustoApplicationRegistration("tenant", "client", "secret");

        using var provider = services.BuildServiceProvider();

        var credential = provider.GetRequiredService<Azure.Core.TokenCredential>();

        Assert.IsType<ClientSecretCredential>(credential);
    }

    private sealed class TestContext : DbContext
    {
        public TestContext(DbContextOptions<TestContext> options)
            : base(options)
        {
        }
    }
}