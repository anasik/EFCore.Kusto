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