<a href='Hidden comment: Image:'></a><img src='http://qizmt.googlecode.com/svn/wiki/images/Qizmt_logo_small.png' alt='Qizmt logo (small)' />

Back to <a href='Hidden comment: Link:'></a>[Wiki Main](Main.md) / [MySpace Qizmt Reference](MySpaceQizmtReference.md)



# Example 1 #
```
List<int> nums = new List<int>();
public virtual void Map(ByteSlice line, MapOutput output)
{
    mstring sLine = mstring.Prepare(line);
    nums.Clear();

    while (sLine.HasNextItem())
    {
        nums.Add(sLine.NextItemToInt32(' '));
    }

    IEnumerable<int> evenNums =
    from n in nums
    where (n % 2) == 0
    select n;

    recordset key = recordset.Prepare();
    key.PutInt(evenNums.Count());

    recordset value = recordset.Prepare();

    foreach (int n in evenNums)
    {
        value.PutInt(n);
    }

    output.Add(key, value);
}
```

# Example 2 #

```
List<int> nums = new List<int>();
public override void Reduce(ByteSlice key, ByteSliceList values, ReduceOutput output)
{
    recordset rKey = recordset.Prepare(key);
    int numCount = rKey.GetInt();

    for (int i = 0; i < values.Length; i++)
    {
        nums.Clear();
        recordset value = recordset.Prepare(values.Items[i]);

        for (int j = 0; j < numCount; j++)
        {
            nums.Add(value.GetInt());
        }

        IEnumerable<int> distinctNums = nums.Distinct();

        int min = distinctNums.Min();
        int max = distinctNums.Max();
        double avg = distinctNums.Average();
        int sum = distinctNums.Aggregate(0, (total, next) => next > 50 ? total + next : total);

        mstring line = mstring.Prepare("Min: ");
        line = line.AppendM(min)
        .AppendM("  Max: ")
        .AppendM(max)
        .AppendM("  Sum over 50: ")
        .AppendM(sum)
        .AppendM("  Avg: ")
        .AppendM(avg);

        output.Add(line);
    }
}
```