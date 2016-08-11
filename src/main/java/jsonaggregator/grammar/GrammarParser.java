package jsonaggregator.grammar;

import java.util.List;

import org.apache.beam.sdk.repackaged.com.google.common.base.Preconditions;

import com.wizzardo.tools.json.JsonTools;

public class GrammarParser {

  public static Grammar getGrammar(final String jsonRepresentation)
      throws IllegalStateException, NullPointerException {

    final Grammar grammar = JsonTools.parse(jsonRepresentation, Grammar.class);

    Preconditions.checkNotNull(grammar.fields, "fields list must be present");

    validateFields(grammar.getFields());

    return grammar;
  }

  private static void validateFields(final List<Field> fields)
      throws IllegalStateException, NullPointerException {

    Preconditions.checkState(fields.size() > 0, "fields list must contain at least one element");

    for (final Field field : fields) {

      validateRequiredFields(field);
    }
  }

  private static void validateRequiredFields(final Field field)
      throws NullPointerException, IllegalStateException {

    Preconditions.checkNotNull(field.name, "name field must be present");
    Preconditions.checkNotNull(field.directions, "directions field must be present");
    Preconditions.checkNotNull(field.srcName, "srcName field must be present");
  }
}
