using System;
using System.Threading.Tasks;
using Azure.Core;
using Azure.Identity;
using EFCore.Kusto.Extensions;
using EFCore.Kusto.Infrastructure;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.Kusto.Tests;

public class KustoCredentialTests
{
    private const string ClusterVariable = "KUSTO_TEST_CLUSTER";
    private const string DatabaseVariable = "KUSTO_TEST_DATABASE";
    private const string QueryVariable = "KUSTO_TEST_QUERY";
    private const string ManagedIdentitySwitch = "KUSTO_TEST_USE_MANAGED_IDENTITY";
    private const string ManagedIdentityClientIdVariable = "KUSTO_TEST_MANAGED_IDENTITY_CLIENT_ID";
    private const string ApplicationTenantIdVariable = "KUSTO_TEST_TENANT_ID";
    private const string ApplicationClientIdVariable = "KUSTO_TEST_CLIENT_ID";
    private const string ApplicationClientSecretVariable = "KUSTO_TEST_CLIENT_SECRET";
    private const string TokenCredentialSwitch = "KUSTO_TEST_USE_AZURE_CLI_TOKEN";

    [Fact]
    public async Task DefaultAzureCredential_can_query_cluster()
    {
        var config = KustoIntegrationConfig.LoadOrSkip();

        await using var context = config.CreateContext(_ => { });
        var rows = await ExecuteIntegrationQueryAsync(context, config.Query);

        Assert.NotEqual(0, rows);
    }

    [Fact]
    public async Task ManagedIdentity_can_query_cluster()
    {
        var config = KustoIntegrationConfig.LoadOrSkip(ManagedIdentitySwitch);

        await using var context = config.CreateContext(kusto =>
            kusto.UseManagedIdentity(Environment.GetEnvironmentVariable(ManagedIdentityClientIdVariable)));

        var rows = await ExecuteIntegrationQueryAsync(context, config.Query);

        Assert.NotEqual(0, rows);
    }

    [Fact]
    public async Task ApplicationRegistration_can_query_cluster()
    {
        var config = KustoIntegrationConfig.LoadOrSkip(ApplicationTenantIdVariable, ApplicationClientIdVariable,
            ApplicationClientSecretVariable);

        var tenantId = GetRequiredSecret(ApplicationTenantIdVariable);
        var clientId = GetRequiredSecret(ApplicationClientIdVariable);
        var clientSecret = GetRequiredSecret(ApplicationClientSecretVariable);

        await using var context = config.CreateContext(kusto =>
            kusto.UseApplicationAuthentication(tenantId, clientId, clientSecret));

        var rows = await ExecuteIntegrationQueryAsync(context, config.Query);

        Assert.NotEqual(0, rows);
    }

    [Fact]
    public async Task TokenCredential_can_query_cluster()
    {
        var config = KustoIntegrationConfig.LoadOrSkip(TokenCredentialSwitch);

        TokenCredential credential = new AzureCliCredential();

        await using var context = config.CreateContext(kusto => kusto.UseTokenCredential(credential));

        var rows = await ExecuteIntegrationQueryAsync(context, config.Query);

        Assert.NotEqual(0, rows);
    }

    private static async Task<int> ExecuteIntegrationQueryAsync(DbContext context, string query)
    {
        await using var command = context.Database.GetDbConnection().CreateCommand();
        command.CommandText = query;

        await context.Database.OpenConnectionAsync();
        await using var reader = await command.ExecuteReaderAsync();

        var rowCount = 0;
        while (await reader.ReadAsync())
        {
            rowCount++;
        }

        return rowCount;
    }

    private static string GetRequiredSecret(string variable)
    {
        var value = Environment.GetEnvironmentVariable(variable);

        if (string.IsNullOrWhiteSpace(value))
        {
            throw new Exception($"Set {variable} to run this integration test.");
        }

        return value;
    }

    private sealed class KustoIntegrationConfig
    {
        private KustoIntegrationConfig(string clusterUrl, string database, string query)
        {
            ClusterUrl = clusterUrl;
            Database = database;
            Query = string.IsNullOrWhiteSpace(query) ? "print 1" : query;
        }

        public string ClusterUrl { get; }
        public string Database { get; }
        public string Query { get; }

        public static KustoIntegrationConfig LoadOrSkip(params string[] requiredVariables)
        {
            var cluster = Environment.GetEnvironmentVariable(ClusterVariable);
            var database = Environment.GetEnvironmentVariable(DatabaseVariable);
            var query = Environment.GetEnvironmentVariable(QueryVariable) ?? "print 1";

            if (string.IsNullOrWhiteSpace(cluster) || string.IsNullOrWhiteSpace(database))
            {
                throw new Exception(
                    $"Set {ClusterVariable} and {DatabaseVariable} to run integration tests against a live Kusto cluster.");
            }

            foreach (var variable in requiredVariables)
            {
                if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(variable)))
                {
                    throw new Exception($"Set {variable} to run this integration test.");
                }
            }

            return new KustoIntegrationConfig(cluster, database, query);
        }

        public DbContext CreateContext(Action<KustoDbContextOptionsBuilder> configureKusto)
        {
            var optionsBuilder = new DbContextOptionsBuilder<IntegrationContext>();
            optionsBuilder.UseKusto(ClusterUrl, Database, configureKusto);
            return new IntegrationContext(optionsBuilder.Options);
        }
    }

    private sealed class IntegrationContext : DbContext
    {
        public IntegrationContext(DbContextOptions<IntegrationContext> options)
            : base(options)
        {
        }
    }
}