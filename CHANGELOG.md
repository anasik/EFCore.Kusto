# Changelog

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
