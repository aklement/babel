package babel.content.eqclasses.properties.topics;

import java.util.ArrayList;
import java.util.List;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Property;

public class Topics extends Property
{   
  public Topics()
  {
    m_clusterIds = new ArrayList<Integer>();
  }
  
  public List<Integer> getClsuterIds()
  {
    return m_clusterIds;
  }
  
  public int commonTopics(Topics other)
  {
    int count = 0;
    
    for (Integer myClusterId : m_clusterIds)
    {
      for (Integer otherClusterId : other.m_clusterIds)
      {
        if (myClusterId.equals(otherClusterId))
        { count++;
        }
      }
    }
    
    return count;
  }

  /**
   * @return true iff the equivalence class did not appear in this topic before
   */
  public boolean addTopic(Integer topicId)
  {
    boolean added = false;
    
    if (added = !m_clusterIds.contains(topicId))
    { m_clusterIds.add(topicId);
    }
  
    return added;
  }
  
  protected ArrayList<Integer> m_clusterIds;

  @Override
  public String persistToString()
  {
    // TODO: Finish
    return null;
  }

  @Override
  public void unpersistFromString(EquivalenceClass eq, String str) throws Exception
  {
    // TODO: Finish
  }
}

