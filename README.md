# EFCore.Kusto

[![NuGet Version](https://img.shields.io/nuget/v/EFCore.Kusto.svg)](https://www.nuget.org/packages/EFCore.Kusto/)

A lightweight, extensible Entity Framework Core provider for translating LINQ queries into **Kusto Query Language (KQL)** AKA **Azure Data Explorer (ADX)**.

While I primarily built this to integrate with ASP.NET Core OData (v8+) for analytical workloads, it can be used
standalone for any LINQ-to-KQL translation needs.

---

## Table of Contents

- [1. Installation](#1-installation)
- [2. Getting Started](#2-getting-started)
- [3. Project Goals](#3-project-goals)
- [4. Read Capabilities](#4-read-capabilities)
- [5. Write Capabilities](#5-write-capabilities)
- [6. Changelog](#6-changelog)
- [7. Contributing](#7-contributing)
- [8. License](#8-license)
- [9. Disclaimer](#9-disclaimer)

---

## 1. Installation

Install the package from NuGet:

```bash
dotnet add package EFCore.Kusto
```
Or via csproj:
```xml
<ProjectReference Include="src/EFCore.Kusto/EFCore.Kusto.csproj" />
```

---

## 2. Getting Started

1. **Register your DbContext** with the Kusto provider and pick an authentication option:

   ```csharp
   builder.Services.AddDbContext<PropertyContext>((sp, options) =>
   {
       options.UseKusto(
           clusterUrl: "https://<cluster>.kusto.windows.net",
           database: "<database>",
           kusto => kusto.UseManagedIdentity());
   });
   ```

    - Use `UseManagedIdentity(clientId)` for a user-assigned identity, or omit the client id for system-assigned
      identities.
    - Use `UseApplicationAuthentication(tenantId, clientId, clientSecret)` for app registrations.
    - Use `UseTokenCredential(credential)` to supply any `TokenCredential` (e.g., one registered
      via `AddKustoManagedIdentityCredential` or `AddKustoApplicationRegistration`).
    - If no authentication option is configured, the provider falls back to `DefaultAzureCredential` when executing
      queries.

2. **Optional: register shared credentials** so they can be reused when building `DbContext` options:

   ```csharp
   builder.Services.AddKustoManagedIdentityCredential(clientId: "<client-id>");
   // or
   builder.Services.AddKustoApplicationRegistration(
       tenantId: "<tenant-id>",
       clientId: "<client-id>",
       clientSecret: "<client-secret>");
   ```

   These helpers register a singleton `TokenCredential` you can inject when calling `UseTokenCredential`
   inside `AddDbContext`.

---

## 3. Project Goals

- Provide a reliable LINQ-to-KQL translation layer.
- Integrate cleanly with ASP.NET Core OData (v8+).
- Offer predictable, debuggable SQL generation.
- Ensure correctness and performance for high‑volume analytical datasets.
- Remain lightweight with minimal runtime overhead.

---

## 4. Read Capabilities

### Query Translation

This provider currently supports:

- `Where` filters
- `Select` projections
- Ordering (`OrderBy`, `ThenBy`)
- Pagination (`Skip`, `Take`)
- Basic join translation used by OData `$expand`
- Counts as used by OData `$count`

### OData Compatibility

EFCore.Kusto works well with:

- `$filter`
- `$select`
- `$orderby`
- `$count`
- `$skip`, `$top`
- `$expand` (entity relationships)

If specific OData query shapes cause issues, they can be addressed case‑by‑case. Community reports are welcome.

---
## 5. Write Capabilities

EFCore.Kusto supports data modification using Kusto-native control commands.

### Supported Operations

- Insert (`Add`, `AddRange`) via `.ingest`
- Update (`Update`) via `.update table`
- Delete (`Remove`, `RemoveRange`) via `.delete table`

### Batching Semantics

- Commands are batched per entity type and target table
- Read and write operations are never mixed in the same batch
- Each batch is executed as a single Kusto command

Note: Transactional guarantees and concurrency semantics are constrained by Kusto’s execution model.

---

## 6. Changelog

See [CHANGELOG.md](./CHANGELOG.md) for a detailed version history.

---

## 7. Contributing

Contributions are welcome.  
If you encounter a translation issue, please include:

1. The LINQ expression (or OData URL if applicable).
2. The expected KQL.
3. The generated KQL (if available).

This helps isolate translation gaps quickly.

---

## 8. License

MIT License – simple, permissive, widely accepted.

EFCore.Kusto is free for commercial and open‑source use.

---

## 9. Disclaimer

While this provider is functional and under active development, it is not yet battle-tested in production environments.

If you encounter unexpected behavior, open an issue — the goal is full reliability for production workloads.
