# Changelog

## [Unreleased]
### Changed
- Batched updates now send the changed values as an inline `datatable` joined on the key, instead of one `union` leg per row. The old shape grew the query-operator count with the batch size: 1000 rows became a ~470 KB command with 1000 legs, measured at ~35 s and ~30 CPU-seconds against a 443-column table; the same batch now takes ~2 s and ~1 CPU-second. Each row carries only the columns it changed, and any column it does not mention keeps its live value, so two rows in one batch may change different columns without overwriting each other. `bag_has_key` is used rather than a null check so that clearing a column stays distinguishable from leaving it alone.
- Batched updates are sent as a single `.execute database script` whose `.update` commands each stay within a maximum length, instead of one `.update` command that Kusto rejected with `SYN0009` once it passed 2,097,152 characters. A command takes as many rows as fit and the rest continue in the next command of the same script, so a batch just over the limit becomes one full command and a small one. The maximum is set with `UseMaxUpdateCommandLength` and defaults to 2,095,674: Kusto measures an `.update` command as 454 characters plus the table name's length longer than its text, so a command of exactly 2,097,152 is still rejected, and 2,095,674 stays within the limit for any table name up to Kusto's 1,024-character maximum. The script is not transactional: if a command fails, the commands before it stay applied, and the error names the failing command.

## [0.2.11]
### Fixed
- `Any(predicate)`/`All(predicate)` over a shadow array-column property (`EF.Property<T>(entity, "col").AsQueryable().Any/All(...)`) silently discarded the predicate whenever it wasn't a single equality/inequality against one constant, collapsing to "array is non-empty" regardless of what the predicate actually checked. Compound predicates (`Any(a => a == x || a == y)`, `All(a => a != x && a != y)`) now translate correctly into repeated array-membership checks; anything outside that shape (mixed `&&`/`||`, ranges, method calls) now throws `NotSupportedException` instead of silently returning the wrong answer.
- `Queryable.Contains` over the same shadow array-column property mis-typed a parameterized non-`string` value (e.g. a captured `int`), binding it with `DbType = String` instead of its correct type.

## [0.2.10]
### Fixed
- `string.Contains`/`StartsWith`/`EndsWith` (the plain single-`string`-argument overloads) now translate to Kusto's native `contains_cs`/`startswith_cs`/`endswith_cs` operators. Previously unsupported: the call fell through untranslated, throwing `NotSupportedException` — notably breaking OData's `$filter=contains(...)` (and `startswith`/`endswith`) query functions. The `_cs` (case-sensitive) operator variants are used to match C#'s case-sensitive default semantics and this provider's existing case-sensitive `==`/`strcmp`-based comparisons; Kusto's plain `contains`/`startswith`/`endswith` are case-insensitive by default.

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
