using System.Data;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.Kusto.Storage;

public sealed class KustoTypeMappingSource : RelationalTypeMappingSource
{
    private static readonly RelationalTypeMapping _string
        = new StringTypeMapping("string", DbType.String);

    private static readonly RelationalTypeMapping _int
        = new IntTypeMapping("int", System.Data.DbType.Int32);
    
    private static readonly RelationalTypeMapping _long
        = new LongTypeMapping("long", System.Data.DbType.Int64);

    private static readonly RelationalTypeMapping _bool
        = new BoolTypeMapping("bool");

    private static readonly RelationalTypeMapping _double
        = new DoubleTypeMapping("real");

    private static readonly RelationalTypeMapping _decimal = new DecimalTypeMapping("real");

    private static readonly RelationalTypeMapping _dateTime
        = new DateTimeTypeMapping("datetime");

    private static readonly RelationalTypeMapping _guid
        = new GuidTypeMapping("string"); // stored as string in Kusto params

    public KustoTypeMappingSource(
        TypeMappingSourceDependencies dependencies,
        RelationalTypeMappingSourceDependencies relationalDependencies)
        : base(dependencies, relationalDependencies)
    {
    }

    protected override RelationalTypeMapping? FindMapping(in RelationalTypeMappingInfo mappingInfo)
    {
        var clrType = mappingInfo.ClrType;

        if (clrType == typeof(string))
            return _string;

        if (clrType == typeof(int))
            return _int;
        
        if ( clrType == typeof(long))
            return _long;

        if (clrType == typeof(bool))
            return _bool;

        if (clrType == typeof(double) || clrType == typeof(float))
            return _double;
        
        if (clrType == typeof(decimal))
            return _decimal;

        if (clrType == typeof(DateTime))
            return _dateTime;

        if (clrType == typeof(Guid))
            return _guid;
        
        if(clrType == typeof(byte[]))
            return new ByteArrayTypeMapping("string", DbType.String); // stored as Base64 string

        // EF fallback
        return base.FindMapping(mappingInfo);
    }
}