# EFCore.Kusto

A lightweight, extensible Entity Framework Core provider for translating LINQ queries into **Kusto Query Language (KQL)** AKA **Azure Data Explorer (ADX)**.

While I primarily built this to integrate with ASP.NET Core OData (v8+) for analytical workloads, it can be used standalone for any LINQ-to-KQL translation needs.

---

## 1. Project Goals

- Provide a reliable LINQ-to-KQL translation layer.
- Integrate cleanly with ASP.NET Core OData (v8+).
- Offer predictable, debuggable SQL generation.
- Ensure correctness and performance for high‑volume analytical datasets.
- Remain lightweight with minimal runtime overhead.

---

## 2. Current Capabilities

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

## 3. Getting Started

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

   - Use `UseManagedIdentity(clientId)` for a user-assigned identity, or omit the client id for system-assigned identities.【F:src/EFCore.Kusto/Extensions/KustoDbContextOptionsBuilderExtensions.cs†L68-L112】
   - Use `UseApplicationAuthentication(tenantId, clientId, clientSecret)` for app registrations.【F:src/EFCore.Kusto/Extensions/KustoDbContextOptionsBuilderExtensions.cs†L84-L97】
   - Use `UseTokenCredential(credential)` to supply any `TokenCredential` (e.g., one registered via `AddKustoManagedIdentityCredential` or `AddKustoApplicationRegistration`).【F:src/EFCore.Kusto/Extensions/KustoDbContextOptionsBuilderExtensions.cs†L102-L113】【F:src/EFCore.Kusto/Extensions/KustoServiceCollectionExtensions.cs†L55-L76】
   - If no authentication option is configured, the provider falls back to `DefaultAzureCredential` when executing queries.【F:src/EFCore.Kusto/Data/KustoCommand.cs†L42-L55】

2. **Optional: register shared credentials** so they can be reused when building `DbContext` options:

   ```csharp
   builder.Services.AddKustoManagedIdentityCredential(clientId: "<client-id>");
   // or
   builder.Services.AddKustoApplicationRegistration(
       tenantId: "<tenant-id>",
       clientId: "<client-id>",
       clientSecret: "<client-secret>");
   ```

   These helpers register a singleton `TokenCredential` you can inject when calling `UseTokenCredential` inside `AddDbContext`.

---

## 4. Installation

NuGet package publishing is planned. Until then, clone the repository and reference the project directly:

```xml
<ProjectReference Include="src/EFCore.Kusto/EFCore.Kusto.csproj" />
```

---

## 5. Usage Example

```csharp
var results = await db.Property
    .Where(p => p.ListingKey == "123")
    .Select(p => new { p.ListingKey, p.PendingDate })
    .ToListAsync();
```

Produces KQL similar to:

```kusto
Property
| where ListingKey == "123"
| project ListingKey, PendingDate
```

### OData Example

```csharp
[EnableQuery]
public IQueryable<Property> Get() => _context.Property;
```

Standard OData query options are supported out of the box.

---

## 6. Contributing

Contributions are welcome.  
If you encounter a translation issue, please include:

1. The LINQ expression (or OData URL if applicable).
2. The expected KQL.
3. The generated KQL (if available).

This helps isolate translation gaps quickly.

---

## 7. License

MIT License – simple, permissive, widely accepted.

EFCore.Kusto is free for commercial and open‑source use.

---

## 8. Disclaimer

While this provider is functional and under active development, it is not yet battle-tested in production environments.

If you encounter unexpected behavior, open an issue — the goal is full reliability for production workloads.