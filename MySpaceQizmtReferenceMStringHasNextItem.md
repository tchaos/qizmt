<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Non-static methods of mstring](MySpaceQizmtReferenceMStringMethods.md)



# `HasNextItem` #
`public bool HasNextItem()`

Returns true if there is a next item in this mstring.

### Example ###
```
/*
Sample Input:

Sam,1498,321,45,7,8,9,0
Jolie,1,2,3,4,5,6,7,8,9,0,1,2,3,4
John,61,42
May,1
*/

public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    mstring name = sLine.NextItemToString(',');
    int sum = 0;

    while (sLine.HasNextItem())
    {
        int amt = sLine.NextItemToInt(',');
        sum += amt;

    }

    recordset rValue = recordset.Prepare();
    rValue.PutInt(sum);

    output.Add(name, rValue);
} 
```


---




`public bool HasNextItem(char delimiter)`

Returns true if there is a next item in this mstring delimited by the specified delimiter.

### Example ###
```
/*
Sample Input:
Sam;1,2;a,b,c
Jolie;4,5,6;r,s,t
John;7;x
*/

    public virtual void Map(ByteSlice line, MapOutput output)
    {
        mstring sLine = mstring.Prepare(line);
        mstring name = sLine.NextItemToString(';');
        int sum = 0;

        while (sLine.HasNextItem(','))
        {
            int amt = sLine.NextItemToInt(',');
            sum += amt;
        }

        recordset rValue = recordset.Prepare();
        rValue.PutInt(sum);

        output.Add(name, rValue);
    }

```