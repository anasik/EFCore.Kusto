namespace EFCore.Kusto.Query.Internal;

public static class KustoParameterCache
{
    private static readonly AsyncLocal<Dictionary<string, object>> _local
        = new();

    public static Dictionary<string, object> Values
    {
        get
        {
            if (_local.Value == null)
                _local.Value = new Dictionary<string, object>();

            return _local.Value;
        }
    }

    public static void Reset()
        => _local.Value = null;
}
