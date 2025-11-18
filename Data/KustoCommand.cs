using System.Collections;
using System.Data;
using System.Data.Common;
using EFCore.Kusto.Storage;
using Kusto.Data.Common;

namespace EFCore.Kusto;

public sealed class KustoCommand : DbCommand
{
    private readonly KustoConnection _connection;
    private readonly KustoParameterCollection _parameters = new();

    public KustoCommand(KustoConnection connection)
    {
        _connection = connection;
    }

    public override string CommandText { get; set; } = "";
    public override int CommandTimeout { get; set; } = 30;
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override bool DesignTimeVisible { get; set; }
    public override UpdateRowSource UpdatedRowSource { get; set; }

    protected override DbParameterCollection DbParameterCollection => _parameters;

    protected override DbConnection DbConnection
    {
        get => _connection.DbConnection; // fake
        set { }
    }

    protected override DbTransaction? DbTransaction { get; set; }

    public override void Cancel() { }

    public override int ExecuteNonQuery() => 0;

    public override object ExecuteScalar() => null;

    public override void Prepare() { }

    protected override DbParameter CreateDbParameter()
        => new KustoParameter();

    // --------------------------------------------------------------------
    // REAL EXECUTION
    // --------------------------------------------------------------------
    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        string paramDecl = BuildParameterDeclaration();
        string query = paramDecl + CommandText;

        var props = BuildClientRequestProperties();

        var reader = _connection.Client.ExecuteQuery(query, props);

        return new KustoDataReader(reader);
    }

    // --------------------------------------------------------------------
    // BUILD: declare query_parameters(...)
    // --------------------------------------------------------------------
    private string BuildParameterDeclaration()
    {
        if (_parameters.Count == 0)
            return "";

        var parts = _parameters.Parameters
            .Select(p => $"{p.ParameterName}:{ToKustoType(p.DbType)}");

        return "declare query_parameters(" + string.Join(", ", parts) + ");\n";
    }

    private string ToKustoType(DbType type) => type switch
    {
        DbType.Int32 => "long",
        DbType.Int64 => "long",
        DbType.String => "string",
        DbType.DateTime => "datetime",
        DbType.Boolean => "bool",
        DbType.Double => "real",
        _ => "string"
    };

    // --------------------------------------------------------------------
    // BUILD: ClientRequestProperties.Parameters
    // --------------------------------------------------------------------
    private ClientRequestProperties BuildClientRequestProperties()
    {
        var props = new ClientRequestProperties();

        foreach (KustoParameter p in _parameters.Parameters)
        {
            string val = ToKustoLiteral(p.Value, p.DbType);
            props.SetParameter(p.ParameterName, val);
        }

        return props;
    }

    private string ToKustoLiteral(object value, DbType type)
    {
        if (value == null)
            return "null";

        return type switch
        {
            DbType.Int32 => $"long({value})",
            DbType.Int64 => $"long({value})",
            DbType.String => $"\"{value}\"",
            DbType.DateTime => $"datetime({((DateTime)value).ToString("o")})",
            DbType.Boolean => ((bool)value) ? "true" : "false",
            DbType.Double => $"real({value})",
            _ => $"\"{value}\""
        };
    }
}

public sealed class KustoParameterCollection : DbParameterCollection
{
    private readonly List<DbParameter> _params = new();

    public IReadOnlyList<DbParameter> Parameters => _params;

    public override int Count => _params.Count;
    public override object SyncRoot => ((ICollection)_params).SyncRoot;

    public override int Add(object value)
    {
        _params.Add((DbParameter)value);
        return _params.Count - 1;
    }

    public override void AddRange(Array values)
    {
        foreach (var v in values)
            _params.Add((DbParameter)v);
    }

    public override void Clear() => _params.Clear();

    public override bool Contains(object value) => _params.Contains((DbParameter)value);

    public override bool Contains(string value)
        => _params.Any(p => p.ParameterName == value);

    public override void CopyTo(Array array, int index)
        => ((ICollection)_params).CopyTo(array, index);

    public override IEnumerator GetEnumerator() => _params.GetEnumerator();

    public override int IndexOf(object value) => _params.IndexOf((DbParameter)value);

    public override int IndexOf(string parameterName)
        => _params.FindIndex(p => p.ParameterName == parameterName);

    public override void Insert(int index, object value)
        => _params.Insert(index, (DbParameter)value);

    public override void Remove(object value) => _params.Remove((DbParameter)value);

    public override void RemoveAt(int index) => _params.RemoveAt(index);

    public override void RemoveAt(string parameterName)
    {
        var i = IndexOf(parameterName);
        if (i >= 0)
            _params.RemoveAt(i);
    }

    protected override DbParameter GetParameter(int index)
        => _params[index];

    protected override DbParameter GetParameter(string parameterName)
        => _params.First(p => p.ParameterName == parameterName);

    protected override void SetParameter(int index, DbParameter value)
        => _params[index] = value;

    protected override void SetParameter(string parameterName, DbParameter value)
    {
        var i = IndexOf(parameterName);
        _params[i] = value;
    }
}

public sealed class KustoParameter : DbParameter
{
    public override DbType DbType { get; set; }
    public override ParameterDirection Direction { get; set; } = ParameterDirection.Input;
    public override bool IsNullable { get; set; }
    public override string ParameterName { get; set; } = "";
    public override string SourceColumn { get; set; }
    public override object? Value { get; set; }
    public override bool SourceColumnNullMapping { get; set; }
    public override int Size { get; set; }

    public override void ResetDbType() { }
}
