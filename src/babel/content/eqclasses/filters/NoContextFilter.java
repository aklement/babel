package babel.content.eqclasses.filters;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.context.Context;

public class NoContextFilter implements EquivalenceClassFilter
{  
  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass)
  {
    Context context = (eqClass != null) ? (Context)eqClass.getProperty(Context.class.getName()) : null;
    return (context != null) && (context.size() > 0);
  }  
}

