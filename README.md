# EFCore.Kusto

A lightweight, extensible Entity Framework Core provider for translating LINQ queries into **Kusto Query Language (KQL)** and executing them against **Azure Data Explorer (ADX)**.

EFCore.Kusto is engineered for real-world .NET applications that require analytical querying through EF Core while retaining the expressive power of LINQ. It is designed for use both as a standalone EF provider *and* as a backend for **ASP.NET Core OData**, where it delivers clean query translation with minimal friction.

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
- Server‑side parameterization (safe across async flows)
- Literal generation for Kusto value types

### OData Compatibility
While not originally designed as an OData provider, EFCore.Kusto works well with:

- `$filter`
- `$select`
- `$orderby`
- `$skip`, `$top`
- `$expand` (entity relationships)

If specific OData query shapes cause issues, they can be addressed case‑by‑case. Community reports are welcome.

---

## 3. Repository Structure

```
src/
    EFCore.Kusto/           # Provider implementation
tests/                     # Test suite (coming soon)
```

A dedicated test project will be added shortly to ensure robust behavior across edge cases and evolving LINQ patterns.

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

## 6. Roadmap

Short‑term:
- Add full automated test suite.
- Expand join and `$expand` semantics.
- Improve edge-case translation around nested subqueries.
- Package for NuGet.

Long‑term:
- Broader LINQ operator coverage.
- Comprehensive OData compatibility matrix.
- Performance instrumentation and benchmarking.

---

## 7. Contributing

Contributions are welcome.  
If you encounter a translation issue, please include:

1. The LINQ expression.
2. The expected KQL.
3. The generated KQL (if available).
4. Any relevant OData URL if applicable.

This helps isolate translation gaps quickly.

---

## 8. License

MIT License – simple, permissive, widely accepted.

EFCore.Kusto is free for commercial and open‑source use.

---

## 9. Disclaimer

This provider is functional and under active development.  
Kusto is not a relational engine, and certain EF Core patterns may require custom translation.  
If you encounter unexpected behavior, open an issue — the goal is full reliability for production workloads.