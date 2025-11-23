namespace EFCore.Kusto.Infrastructure.Internal;

/// <summary>
/// Describes how the provider should obtain an access token for Azure Data Explorer.
/// </summary>
public enum KustoAuthenticationStrategy
{
    /// <summary>
    /// Use <see cref="Azure.Identity.DefaultAzureCredential"/> to automatically select the credential chain.
    /// </summary>
    DefaultAzureCredential,

    /// <summary>
    /// Use <see cref="Azure.Identity.ManagedIdentityCredential"/> with an optional client id.
    /// </summary>
    ManagedIdentity,

    /// <summary>
    /// Use a registered application and client secret to authenticate.
    /// </summary>
    Application
}
