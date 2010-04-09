package babel.util.misc;

public class EditDistance
{
  public static int distance(String s, String t)
  {
    int d[][];
    int n = s.length();
    int m = t.length();
    int i, j;
    char s_i, t_j;
    int cost;
    
    if (n == 0) 
    { return m;
    }
    if (m == 0)
    { return n;
    }
    
    d = new int[n+1][m+1];

    for (i = 0; i <= n; i++)
    { d[i][0] = i;
    }

    for (j = 0; j <= m; j++)
    { d[0][j] = j;
    }

    for (i = 1; i <= n; i++)
    {
      s_i = s.charAt(i - 1);

      for (j = 1; j <= m; j++)
      {
        t_j = t.charAt(j - 1);

        cost = (s_i == t_j) ? 0 : 1;
        d[i][j] = min(d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1] + cost);
      }
    }

    return d[n][m];
  }

  private static int min(int a, int b, int c)
  {
    int min = a;
    
    if (b < min)
    { min = b;
    }
    if (c < min)
    { min = c;
    }
      
    return min;
  }
}
