package grammar;

import java.io.Serializable;

public class Field implements Serializable {

  private static final long serialVersionUID = -8371594243847279759L;

  // Field Name
  String name;

  // Directions to retrieve the fieldValue
  String directions;

  // Soruce field name
  String srcName;

  // Multiple retrieval
  boolean multipleRetrieval;

  public String getName() {
    return name;
  }

  public String getDirections() {
    return directions;
  }

  public String getSrcName() {
    return srcName;
  }

  public boolean isMultipleRetrieval() {
    return multipleRetrieval;
  }


}
