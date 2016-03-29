<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md) / [Methods of ReduceOutput](MySpaceQizmtReferenceReduceOutputMethods.md)



# `Add` #
`public void Add(ByteSlice entry)`

Adds entry’s buffer to output file with a new line

### Example 1 ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
        sValue += "," + values[i].Value.ToString();

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
} 
```


### Example 2 ###
This example demonstrates what happens when changing the buffer after the ByteSlice has already been created, but before it is added to the

RandomAccesOutput.  You will see the change in the RandomAccesOutput.

```
//Map code
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(parts[1]));

}

//Reducer code
byte[] buffer = new byte[Qizmt_KeyLength + 4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string s = "key=" + UnpadKey(key).ToString();

    Entry.AsciiToBytes(s, buffer, 0);

    ByteSlice b = ByteSlice.Prepare(buffer);

    //Change one byte in the buffer.  This change will be carried to the output.
    buffer[0] = 0x0;

    output.Add(b);
}
/*
Sample Input:

123,apple
234,lemon
444,berry

Sample Output:

key=123            
key=234             
key=444 
*/
```


### Example 3 ###
This example demonstrates what happens when changing the buffer after the ByteSlice has already been created and after it has been added to the

RandomAccessOutput.  You will NOT see the change in the RandomAccessOutput.

```
//Map code
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), ByteSlice.Prepare(parts[1]));

}

//Reducer code
byte[] buffer = new byte[Qizmt_KeyLength + 4];

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string s = "key=" + UnpadKey(key).ToString();

    Entry.AsciiToBytes(s, buffer, 0);

    ByteSlice b = ByteSlice.Prepare(buffer);

    output.Add(b);

    //Change one byte in the buffer.  This change will NOT be carried to the output.
    buffer[0] = 0x0;

}
/*
Sample Input:

123,apple
234,lemon
444,berry

Sample Output:

key=123            
key=234             
key=444 
*/
```


---




`public void Add(mstring s)`

Writes the mstring to the output file.

### Example ###
```
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    recordset rKey = recordset.Prepare(key);

    int i1 = rKey.GetInt32();
    int i2 = rKey.GetInt32();

    mstring ms = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rValue = recordset.Prepare(values[i].Value);

        Int64 i3 = rValue.GetInt64();
        mstring s = rValue.GetString();
        int i4 = rValue.GetInt32();
        mstring delimiter = mstring.Prepare(",");

        ms.Consume(ref s);
        ms.Consume(ref delimiter);
    }

    output.Add(ms);
} 
```