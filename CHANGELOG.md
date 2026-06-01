# Changelog

## [0.2.4]
### Added
- Experimental EF Core migrations support: schema operations translate to KQL control commands (`.create-merge table`, `.alter-merge table`, `.drop`, `.rename`), with applied migrations tracked in an `EFMigrationsHistory` table. Non-transactional; `.alter column type=` clears column data; relational-only constructs (indexes, FKs, constraints, sequences) are no-ops.

## [0.2.3]
### Added
- `GroupBy` → KQL `summarize` translation. `Sum`/`Min`/`Max`/`Average`/`Count`/`LongCount`, `Count(predicate)` → `countif`, `Distinct().Count()` → `dcount`. Composite keys, multi-aggregate projections, and aggregate-alias `OrderBy` supported.
- Conditional `?:` translation → `iif` (two-way) and `case` (multi-way), including inside aggregates.

### Fixed
- Parameter substitution now emits proper typed KQL literals (strings, dates, GUIDs, nulls were all broken under raw `ToString()`).

## [0.2.2]
### Fixed
- `KustoQuerySqlGenerator` when the same parameter is used multiple times in a query.

## [0.2.1]
### Added
- Support for Hex strings in byte arrays. 

## [0.2.0]
### Added
- Support for `Any` 

## [0.1.9]
### Added
- Support for OUTER APPLY and CROSS APPLY.

## [0.1.8]
### Fixed
- Inequality comparisons on strings.

## [0.1.7]
### Fixed
- `not` operator translation
- Duplicate column issue in joins

### Added
- Support for `Contains`

## [0.1.6]
### Fixed
- NULL handling in PATCH requests.
- String escaping in PATCH requests.

## [0.1.5]
### Optimized
- `.update` command to use less nesting and support larger batches

## [0.1.4]
### Fixed
- `COUNT(*)` regression resulting from `KustoQuerySqlGenerator.WriteProjection` refactor

## [0.1.3]
### Added
- Support for `DateOnly` type translation

## [0.1.2]
### Added
- Write command batching per entity/table

## [0.1.1]
### Added
- Update support via Kusto `.update table` commands

## [0.1.0]
- Initial release (read-only query support)
