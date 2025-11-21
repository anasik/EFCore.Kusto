using Microsoft.EntityFrameworkCore.Diagnostics;

namespace EFCore.Kusto.Diagnostics.Internal;

/// <summary>
/// Provides logging definitions specific to the Kusto provider.
/// </summary>
public sealed class KustoLoggingDefinitions : RelationalLoggingDefinitions;