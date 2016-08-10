package grammar;

import java.io.Serializable;
import java.util.List;

public class Grammar implements Serializable {

  private static final long serialVersionUID = 7961348405780774638L;

  List<Field> fields;

  public List<Field> getFields() {
    return fields;
  }

}
