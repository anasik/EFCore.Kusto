using System.Collections;
using System.Data;
using System.Data.Common;
using Kusto.Data.Common;

namespace EFCore.Kusto.Storage;

public sealed class KustoDataReader : DbDataReader
{
    private readonly IDataReader _inner;

    public KustoDataReader(IDataReader inner) => _inner = inner;

    public override bool Read() => _inner.Read();
    public override int FieldCount => _inner.FieldCount;

    public override string GetName(int ordinal) => _inner.GetName(ordinal);
    public override int GetOrdinal(string name) => _inner.GetOrdinal(name);

    public override object GetValue(int ordinal) => _inner.GetValue(ordinal);
    public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);

    public override bool HasRows => true;
    public override bool IsClosed => false;
    public override int RecordsAffected => -1;
    public override int Depth => 0;

    public override object this[int ordinal] => _inner.GetValue(ordinal);
    public override object this[string name] => _inner.GetValue(_inner.GetOrdinal(name));

    public override IEnumerator GetEnumerator() => throw new NotSupportedException();
    public override bool NextResult() => false;

    public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal);
    public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal);

    public override string GetString(int ordinal) => _inner.GetString(ordinal);
    public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
    public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
    public override double GetDouble(int ordinal) => Convert.ToDouble(_inner.GetValue(ordinal));
    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(_inner.GetValue(ordinal));
    public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal);
    public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
    public override float GetFloat(int ordinal) => Convert.ToSingle(_inner.GetValue(ordinal));
    public override short GetInt16(int ordinal) => Convert.ToInt16(_inner.GetValue(ordinal));
    public override byte GetByte(int ordinal) => Convert.ToByte(_inner.GetValue(ordinal));
    public override Guid GetGuid(int ordinal) => Guid.Parse(_inner.GetValue(ordinal).ToString());

    public override long GetBytes(int a, long b, byte[] c, int d, int e) => throw new NotSupportedException();
    public override char GetChar(int ordinal)
    {
        throw new NotImplementedException();
    }

    public override long GetChars(int a, long b, char[] c, int d, int e) => throw new NotSupportedException();
    public override int GetValues(object[] values)
    {
        int count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++) values[i] = _inner.GetValue(i);
        return count;
    }
}
