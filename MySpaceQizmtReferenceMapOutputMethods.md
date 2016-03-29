<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Methods of `MapOutput` #



## `Add` ##
`public void Add(ByteSlice key, ByteSlice value)`

Adds the key value pair to Map, with the offsets of key and value.

#### Example 1 ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');
    output.Add(ByteSlice.Prepare(parts[0].PadRight(16, '\0')), ByteSlice.Prepare(parts[1]));
} 
```



#### Example 2 ####
This example demonstrates what happens when changing the buffer after the ByteSlice has already been created, but before it is added to the

MapOutput.  You will see the change in the MapOutput.

```
//Map code
List<byte> buffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    buffer.Clear();
    Entry.AsciiToBytesAppend(parts[1], buffer);

    ByteSlice val = ByteSlice.Prepare(buffer);

    //Change one byte in the buffer.  This change will be carried to the MapOutput.
    buffer[0] = 0x0;

    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), val);
}

//Reducer code
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
}
/*
Sample Input:

123,apple
234,lemon
444,berry

Sample Output:

key=123
values=, pple
key=234
values=, emon
key=444
values=, erry
*/
```


#### Example 3 ####
This example demonstrates what happens when changing the buffer after the ByteSlice has already been created and after it has been added to the

MapOutput.  You will NOT see the change in the MapOutput.

```
//Map code
List<byte> buffer = new List<byte>();

public virtual void Map(ByteSlice line, MapOutput output)
{
    string sLine = line.ToString();
    string[] parts = sLine.Split(',');

    buffer.Clear();
    Entry.AsciiToBytesAppend(parts[1], buffer);

    ByteSlice val = ByteSlice.Prepare(buffer);

    output.Add(ByteSlice.PreparePaddedStringAscii(parts[0], Qizmt_KeyLength), val);

    //Then change one byte in the buffer.  This change will NOT be carried to the MapOutput.
    buffer[0] = 0x0;
}

//Reducer code
public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    string sKey = UnpadKey(key).ToString();
    string sValue = "";

    for (int i = 0; i < values.Length; i++)
    {
        sValue += "," + values[i].Value.ToString();
    }

    output.Add(ByteSlice.Prepare("key=" + sKey));
    output.Add(ByteSlice.Prepare("values=" + sValue));
}
/*
Sample Input:

123,apple
234,lemon
444,berry

Sample Output:

key=123
values=,apple
key=234
values=,lemon
key=444
values=,berry
*/
```

---




`public void Add(recordset key, recordset value)`

Adds a key value pair to map.
#### Remarks ####
When key value pair is added to Map using recordset, you must retrieve the key value pair using recordset in the Reduce method.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    //i,i,l,s,i
    //61,92,383,Washington,595
    mstring ms = mstring.Prepare(line);

    int i1 = ms.CsvNextItemToInt32();
    int i2 = ms.CsvNextItemToInt32();
    Int64 i3 = ms.CsvNextItemToInt64();
    mstring s = ms.CsvNextItemToString();
    int i4 = ms.CsvNextItemToInt32();

    recordset rKey = recordset.Prepare();
    recordset rValue = recordset.Prepare();

    rKey.PutInt32(i1);
    rKey.PutInt32(i2);

    rValue.PutInt64(i3);
    rValue.PutString(s);
    rValue.PutInt32(i4);

    output.Add(rKey, rValue);
}

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

---




`public void Add(mstring key, mstring value)`

Adds a key value mstring pair to map.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int64 id = sLine.CsvNextItemToInt64();
    mstring name = sLine.CsvNextItemToString().MToLower();
    Int64 x = sLine.CsvNextItemToInt64();

    Int64 total = id + x;

    mstring key = mstring.Prepare(total);

    output.Add(key, name);
}

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    output.Add(sKey);

    mstring delimiter = mstring.Prepare(",");
    mstring sValues = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        mstring name = mstring.Prepare(values[i].Value);
        sValues = sValues.MAppend(name);
        sValues = sValues.MAppend(delimiter);
    }

    output.Add(sValues);
} 
```

---




`public void Add(mstring key, recordset value)`

Adds a key value pair to map.
#### Remarks ####
Since the value is added to Map using recordset, you must use recordset to retrieve the value back in the Reduce method.

#### Example ####
```
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);

    Int64 id = sLine.CsvNextItemToInt64();
    mstring name = sLine.CsvNextItemToString().MToLower();
    Int64 x = sLine.CsvNextItemToInt64();

    Int64 total = id + x;

    mstring key = mstring.Prepare(total);

    recordset rs = recordset.Prepare();
    rs.PutString(name);
    rs.PutInt64(x);

    output.Add(key, rs);
}

public override void Reduce(ByteSlice key, RandomAccessEntries values, RandomAccessOutput output)
{
    mstring sKey = mstring.Prepare(UnpadKey(key));
    output.Add(sKey);

    mstring delimiter = mstring.Prepare(",");
    mstring sValues = mstring.Prepare();

    for (int i = 0; i < values.Length; i++)
    {
        recordset rs = recordset.Prepare(values[i].Value);
        mstring name = rs.GetString();
        sValues = sValues.MAppend(name);
        sValues = sValues.MAppend(delimiter);
    }

    output.Add(sValues);
} 
```