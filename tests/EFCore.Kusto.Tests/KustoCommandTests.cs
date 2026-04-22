using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using EFCore.Kusto.Data;
using Kusto.Data.Common;
using Xunit;

namespace EFCore.Kusto.Tests;

public class KustoCommandTests
{
    [Fact]
    public void Prepare_command_text_deduplicates_reused_parameter_names()
    {
        var parameters = new FakeDbParameterCollection();
        parameters.Add(new FakeDbParameter("__TypedProperty_0", DbType.Int64, 10));
        parameters.Add(new FakeDbParameter("__TypedProperty_0", DbType.Int64, 10));

        var commandText = KustoCommand.PrepareCommandText(
            "EntityEvents | where EntityEventSequence > TypedProperty_0 | take TypedProperty_0",
            parameters,
            new ClientRequestProperties());

        Assert.StartsWith("declare query_parameters (TypedProperty_0:long);", commandText);
        Assert.Equal(1, CountOccurrences(commandText, "TypedProperty_0:long"));
        Assert.Contains("| where EntityEventSequence > TypedProperty_0", commandText);
        Assert.Contains("| take TypedProperty_0", commandText);
    }

    [Fact]
    public void Prepare_command_text_throws_for_conflicting_duplicate_parameter_names()
    {
        var parameters = new FakeDbParameterCollection();
        parameters.Add(new FakeDbParameter("__TypedProperty_0", DbType.Int64, 10));
        parameters.Add(new FakeDbParameter("__TypedProperty_0", DbType.String, "10"));

        var ex = Assert.Throws<InvalidOperationException>(() => KustoCommand.PrepareCommandText(
            "EntityEvents | take TypedProperty_0",
            parameters,
            new ClientRequestProperties()));

        Assert.Contains("TypedProperty_0", ex.Message);
    }

    [Fact]
    public void Prepare_command_text_keeps_existing_scalar_query_rewrites()
    {
        var prepared = KustoCommand.PrepareCommandText(
            "EntityEvents\n| project COUNT(*)\n| project EXISTS Something",
            new FakeDbParameterCollection(),
            new ClientRequestProperties());

        Assert.Contains("| count", prepared);
        Assert.DoesNotContain("| project COUNT(*)", prepared);
        Assert.DoesNotContain("| project EXISTS ", prepared);
    }

    [Fact]
    public void Normalize_parameter_value_preserves_numeric_parameter_types()
    {
        var normalized = KustoCommand.NormalizeParameterValue(DbType.Int64, 10);

        Assert.IsType<long>(normalized);
        Assert.Equal(10L, normalized);
    }

    [Fact]
    public void Apply_parameter_uses_typed_kusto_parameter_overloads()
    {
        var requestProperties = new ClientRequestProperties();

        KustoCommand.ApplyParameter(requestProperties, "TypedProperty_0", DbType.Int64, 10L);

        Assert.Equal("10", requestProperties.Parameters["TypedProperty_0"]);
    }

    private static int CountOccurrences(string value, string token)
    {
        var count = 0;
        var index = 0;
        while ((index = value.IndexOf(token, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += token.Length;
        }

        return count;
    }

    private sealed class FakeDbParameter(string name, DbType dbType, object? value) : DbParameter
    {
        public override DbType DbType { get; set; } = dbType;
        public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
        public override bool IsNullable { get; set; }
        public override string ParameterName { get; set; } = name;
        public override int Size { get; set; }
        public override string SourceColumn { get; set; } = string.Empty;
        public override object? Value { get; set; } = value;
        public override bool SourceColumnNullMapping { get; set; }

        public override void ResetDbType()
        {
        }
    }

    private sealed class FakeDbParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _items = new();

        public override int Count => _items.Count;
        public override object SyncRoot => this;

        public override int Add(object value)
        {
            _items.Add((DbParameter)value);
            return _items.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
            {
                Add(value!);
            }
        }

        public override void Clear() => _items.Clear();
        public override bool Contains(object value) => _items.Contains((DbParameter)value);
        public override bool Contains(string value) => _items.Any(item => item.ParameterName == value);
        public override void CopyTo(Array array, int index) => _items.ToArray().CopyTo(array, index);
        public override IEnumerator GetEnumerator() => _items.GetEnumerator();
        public override int IndexOf(object value) => _items.IndexOf((DbParameter)value);
        public override int IndexOf(string parameterName) => _items.FindIndex(item => item.ParameterName == parameterName);
        public override void Insert(int index, object value) => _items.Insert(index, (DbParameter)value);
        public override void Remove(object value) => _items.Remove((DbParameter)value);
        public override void RemoveAt(int index) => _items.RemoveAt(index);
        public override void RemoveAt(string parameterName) => _items.RemoveAt(IndexOf(parameterName));
        protected override DbParameter GetParameter(int index) => _items[index];
        protected override DbParameter GetParameter(string parameterName) => _items[IndexOf(parameterName)];
        protected override void SetParameter(int index, DbParameter value) => _items[index] = value;
        protected override void SetParameter(string parameterName, DbParameter value) => _items[IndexOf(parameterName)] = value;
    }
}
