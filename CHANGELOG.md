# Changelog

## [0.2.9]
### Fixed
- Batched deletes (e.g. `RemoveRange`) generated malformed KQL for 2+ rows due to an unbalanced parenthesis in `AppendDeleteOperation`.

## [0.2.8]
### Added
- `string.IsNullOrEmpty`/`IsNullOrWhiteSpace` now translate to `isempty()`/`isempty(trim(...))`. Previously unsupported: the call fell through to EF Core's default expansion (`IsNull(x) OR x == ""`), which this provider's null handling collapsed into a bare `x == ""`, silently missing rows where the column was actually null.
- Opt-in `UseIsEmptyForStringIsNull()` on `KustoDbContextOptionsBuilder`: when enabled, `x.Field == null` / `!= null` on a string-typed operand generates `isempty()`/`isnotempty()` instead of `isnull()`/`isnotnull()`. A Kusto string column can never actually hold a database null, so `isnull()` is structurally always false for one — this switches to Kusto's own recommended idiom instead. Disabled by default; existing `isnull`/`isnotnull` behavior is unchanged unless called.

### Fixed
- KQL has no bare `null` keyword. Null literals reached through a constant or `CASE` branch (e.g. a ternary with an explicit `null` arm) previously rendered as the literal text `null`, which Kusto doesn't recognize. These now render as typed nulls (`int(null)`, `datetime(null)`, `guid(null)`, ...); strings fall back to `""`, since a Kusto string can't represent null at all.

## [0.2.7]
### Added
- Support for inner and right joins (previously only left join was translated).

### Fixed
- `DbType.Decimal` was mapped to Kusto's `real` (binary floating-point) type instead of `decimal`, which could silently lose precision on decimal parameters.
- Control-command routing (`.show`/`.drop`/etc. vs. a query) is now decided once from the provider's own pristine command text, before any `DbCommandInterceptor` can mutate it — closing a gap where a header-prepending interceptor could cause a control command to be misrouted as a query. Commands created outside the EF Core pipeline (e.g. via `DbConnection.CreateCommand()` directly) keep the original execution-time text-sniffing fallback.
- Unrecognized join expression types now throw `NotSupportedException` instead of silently being translated as a left join.

## [0.2.6]
### Fixed
- Regression for count translation introduced in 0.2.3.

## [0.2.5]
### Added
- Multi-targeting for `net8.0`, `net9.0` and `net10.0`, building against EF Core 8, 9 and 10 respectively. EF Core 8 support is retained unchanged.

### Fixed
- Adapted to the EF Core 9 migrations API: `HistoryRepository`'s database-lock members (a no-op lock, since Kusto has no advisory-lock primitive) and the new `IMigrationCommandExecutor` overloads.
- Adapted to the EF Core 10 `RelationalCommand` `logCommandText` constructor parameter.
- Query-parameter rendering now strips the captured-variable `__` prefix only when present, so translation works on EF Core 10 (which dropped the prefix) as well as EF Core 8/9.

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
