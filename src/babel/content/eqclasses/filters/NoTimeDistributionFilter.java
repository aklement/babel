package babel.content.eqclasses.filters;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.time.TimeDistribution;

public class NoTimeDistributionFilter implements EquivalenceClassFilter
{  
  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass)
  {
    TimeDistribution distro = (eqClass != null) ? (TimeDistribution)eqClass.getProperty(TimeDistribution.class.getName()) : null;
    return (distro != null) && (distro.getTotalOccurences() > 0);
  }
}
