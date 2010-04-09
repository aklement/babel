package babel.content.eqclasses.filters;

import java.util.regex.Pattern;

import babel.content.eqclasses.EquivalenceClass;

public class GarbageFilter implements EquivalenceClassFilter
{
  protected static final Pattern GARBAGE_PATTERN = Pattern.compile(".*[`;\\[\\]\\|\\,\\.\"\\'\\{\\}\\?\\)\\(=\\>\\<\\*\\:\\#\\%\\&\\;\\/\\\\].*");
  protected static final Pattern DIGITS_PATTERN = Pattern.compile("[0-9]");

	@Override
	public boolean acceptEquivalenceClass(EquivalenceClass eqClass) 
	{
    return ((eqClass != null) && 
        !GARBAGE_PATTERN.matcher(eqClass.getStem()).find() &&
        !DIGITS_PATTERN.matcher(eqClass.getStem()).find());
	}
}
