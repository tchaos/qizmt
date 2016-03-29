<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `CsvNextItemToDouble` #
`public double CsvNextItemToDouble()`

Get the next Csv double item.

### Example ###
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring sCoords = sLine.MSubstring(2);

    double x = sCoords.CsvNextItemToDouble();
    double y = sCoords.CsvNextItemToDouble();

    double kx = x * (double)5;
    double ky = y * (double)5;

    kx = Math.Truncate(kx);
    ky = Math.Truncate(ky);

    mstring sKey = mstring.Prepare(kx);
    sKey = sKey.MAppend(',').MAppend(ky);

    output.Add(sKey, sLine);
} 
```