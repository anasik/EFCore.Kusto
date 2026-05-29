using System.Text;
using Microsoft.EntityFrameworkCore.Migrations;

namespace EFCore.Kusto.Migrations.Internal;

/// <summary>
/// Shared helpers for emitting a group of Kusto control commands as a single
/// <c>.execute database script</c> batch — the only way Kusto runs multiple management
/// commands in one request. The batch runs sequentially and stops on the first error
/// (<c>ThrowOnErrors=true</c>), which is the closest Kusto offers to applying a migration
/// atomically.
/// </summary>
internal static class KustoMigrationBatch
{
    /// <summary>The opening header of an <c>.execute database script</c> batch (header line + <c>&lt;|</c>).</summary>
    public const string Header = ".execute database script with (ThrowOnErrors=true)\n<|\n";

    /// <summary>
    /// Wraps the given control commands in a single batch. A single command is returned as-is
    /// (no wrapper needed); multiple commands are joined under the batch header, one per line.
    /// Returns <see langword="null"/> when there are no commands.
    /// </summary>
    public static string? Wrap(IReadOnlyList<MigrationCommand> commands)
    {
        if (commands.Count == 0)
        {
            return null;
        }

        if (commands.Count == 1)
        {
            return commands[0].CommandText;
        }

        var builder = new StringBuilder(Header);
        foreach (var command in commands)
        {
            builder.AppendLine(command.CommandText.TrimEnd('\r', '\n'));
        }

        return builder.ToString();
    }
}
