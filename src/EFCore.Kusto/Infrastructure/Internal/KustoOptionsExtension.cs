using Azure.Core;
using EFCore.Kusto.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace EFCore.Kusto.Infrastructure.Internal;

/// <summary>
/// Represents the configurable options for the Kusto EF Core provider.
/// </summary>
public sealed class KustoOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo _info;

    /// <summary>
    /// Gets the configured cluster URL.
    /// </summary>
    public string? ClusterUrl { get; private set; }

    /// <summary>
    /// Gets the configured database name.
    /// </summary>
    public string? Database { get; private set; }

    /// <summary>
    /// Gets the authentication strategy to use when acquiring tokens.
    /// </summary>
    public KustoAuthenticationStrategy AuthenticationStrategy { get; private set; }

    /// <summary>
    /// Gets the optional managed identity client id for authentication.
    /// </summary>
    public string? ManagedIdentityClientId { get; private set; }

    /// <summary>
    /// Gets the application (client) id used for app registration authentication.
    /// </summary>
    public string? ApplicationClientId { get; private set; }

    /// <summary>
    /// Gets the tenant id used for app registration authentication.
    /// </summary>
    public string? ApplicationTenantId { get; private set; }

    /// <summary>
    /// Gets the client secret used for app registration authentication.
    /// </summary>
    public string? ApplicationClientSecret { get; private set; }

    /// <summary>
    /// Gets an explicitly provided credential instance, if any.
    /// </summary>
    public TokenCredential? Credential { get; private set; }

    public KustoOptionsExtension() { }

    private KustoOptionsExtension(KustoOptionsExtension copyFrom)
        : base(copyFrom)
    {
        ClusterUrl = copyFrom.ClusterUrl;
        Database = copyFrom.Database;
        AuthenticationStrategy = copyFrom.AuthenticationStrategy;
        ManagedIdentityClientId = copyFrom.ManagedIdentityClientId;
        ApplicationClientId = copyFrom.ApplicationClientId;
        ApplicationTenantId = copyFrom.ApplicationTenantId;
        ApplicationClientSecret = copyFrom.ApplicationClientSecret;
        Credential = copyFrom.Credential;
    }

    protected override RelationalOptionsExtension Clone()
        => new KustoOptionsExtension(this);

    /// <summary>
    /// Returns a copy of the extension with the cluster URL set.
    /// </summary>
    public KustoOptionsExtension WithCluster(string cluster)
    {
        var clone = new KustoOptionsExtension(this);
        clone.ClusterUrl = cluster;
        return clone;
    }

    /// <summary>
    /// Returns a copy of the extension with the database name set.
    /// </summary>
    public KustoOptionsExtension WithDatabase(string db)
    {
        var clone = new KustoOptionsExtension(this);
        clone.Database = db;
        return clone;
    }

    /// <summary>
    /// Returns a copy of the extension configured to use managed identity authentication.
    /// </summary>
    public KustoOptionsExtension WithManagedIdentity(string? clientId = null)
    {
        var clone = new KustoOptionsExtension(this);
        clone.AuthenticationStrategy = KustoAuthenticationStrategy.ManagedIdentity;
        clone.ManagedIdentityClientId = clientId;
        clone.Credential = null;
        clone.ApplicationClientId = null;
        clone.ApplicationClientSecret = null;
        clone.ApplicationTenantId = null;
        return clone;
    }

    /// <summary>
    /// Returns a copy of the extension configured to use application registration authentication.
    /// </summary>
    public KustoOptionsExtension WithApplicationAuthentication(string tenantId, string clientId, string clientSecret)
    {
        var clone = new KustoOptionsExtension(this);
        clone.AuthenticationStrategy = KustoAuthenticationStrategy.Application;
        clone.ApplicationTenantId = tenantId;
        clone.ApplicationClientId = clientId;
        clone.ApplicationClientSecret = clientSecret;
        clone.Credential = null;
        clone.ManagedIdentityClientId = null;
        return clone;
    }

    /// <summary>
    /// Returns a copy of the extension configured to use an explicit <see cref="TokenCredential"/>.
    /// </summary>
    public KustoOptionsExtension WithTokenCredential(TokenCredential credential)
    {
        var clone = new KustoOptionsExtension(this);
        clone.AuthenticationStrategy = KustoAuthenticationStrategy.DefaultAzureCredential;
        clone.Credential = credential;
        clone.ManagedIdentityClientId = null;
        clone.ApplicationClientId = null;
        clone.ApplicationClientSecret = null;
        clone.ApplicationTenantId = null;
        return clone;
    }

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkKusto();

    public override void Validate(IDbContextOptions options) { }

    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    private sealed class ExtensionInfo : DbContextOptionsExtensionInfo
    {
        private readonly KustoOptionsExtension _extension;

        public ExtensionInfo(KustoOptionsExtension extension)
            : base(extension)
        {
            _extension = extension;
        }

        public override bool IsDatabaseProvider => true;

        public override string LogFragment =>
            $"Kusto(Cluster={_extension.ClusterUrl ?? "null"},Db={_extension.Database ?? "null"},Auth={_extension.AuthenticationStrategy}) ";

        public override int GetServiceProviderHashCode()
            => HashCode.Combine(
                _extension.ClusterUrl,
                _extension.Database,
                _extension.AuthenticationStrategy,
                _extension.ManagedIdentityClientId,
                _extension.ApplicationClientId,
                _extension.ApplicationTenantId,
                _extension.ApplicationClientSecret,
                _extension.Credential?.GetType());

        public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other)
        {
            return other is ExtensionInfo;
        }

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
            debugInfo["Kusto:Cluster"] = _extension.ClusterUrl ?? "null";
            debugInfo["Kusto:Database"] = _extension.Database ?? "null";
            debugInfo["Kusto:Auth"] = _extension.AuthenticationStrategy.ToString();
            debugInfo["Kusto:ManagedIdentityClientId"] = _extension.ManagedIdentityClientId ?? "null";
            debugInfo["Kusto:AppClientId"] = _extension.ApplicationClientId ?? "null";
            debugInfo["Kusto:AppTenantId"] = _extension.ApplicationTenantId ?? "null";
            debugInfo["Kusto:CredentialType"] = _extension.Credential?.GetType().FullName ?? "null";
        }
    }
}
