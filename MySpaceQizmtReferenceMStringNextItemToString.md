<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `NextItemToString` #
`public mstring NextItemToString(char delimiter)`

Get the next string item, delimited by the char delimiter.

### Example ###
```
/*
Sample Input:

123|apple|444
444|lemon|555
54|orange|666
778|lime|777
123|soda|888
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    int i1 = sLine.NextItemToInt32('|');
    mstring value = sLine.NextItemToString('|');
    int i2 = sLine.NextItemToInt32('|');

    int total = i1 + i2;

    mstring key = mstring.Prepare(total);

    output.Add(key, value);
} 
```