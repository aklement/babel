package babel.content.eqclasses.filters;

import java.util.regex.Pattern;

import babel.content.eqclasses.EquivalenceClass;

/** Filters out Equivalence classes containing any roman charachters. */
public class RomanizationFilter implements EquivalenceClassFilter
{
  protected static final Pattern ROMAN_CHARS_PATTERN = Pattern.compile("[a-zA-Z]");

  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass) 
  {
    return ((eqClass != null) && !ROMAN_CHARS_PATTERN.matcher(eqClass.getStem()).find());
  }
}