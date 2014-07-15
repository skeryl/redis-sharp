namespace RedisSharp
{
  public class SortOptions
  {
    public string Key { get; set; }

    public bool Descending { get; set; }

    public bool Lexographically { get; set; }

    public int LowerLimit { get; set; }

    public int UpperLimit { get; set; }

    public string By { get; set; }

    public string StoreInKey { get; set; }

    public string Get { get; set; }

    public string ToCommand()
    {
      string str = "SORT " + Key;
      if (LowerLimit != 0 || UpperLimit != 0)
        str = str + (object) " LIMIT " + (string) (object) LowerLimit + " " + (string) (object) UpperLimit;
      if (Lexographically)
        str = str + " ALPHA";
      if (!string.IsNullOrEmpty(By))
        str = str + " BY " + By;
      if (!string.IsNullOrEmpty(Get))
        str = str + " GET " + Get;
      if (!string.IsNullOrEmpty(StoreInKey))
        str = str + " STORE " + StoreInKey;
      return str;
    }
  }
}
