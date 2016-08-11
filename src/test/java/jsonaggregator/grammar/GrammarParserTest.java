package jsonaggregator.grammar;

import org.junit.Test;

public class GrammarParserTest {

  @Test
  public void testSemanticallyCorrectGrammarRepr() {

    final String grammarJsonRepr = "{\"fields\":["
        + "{\"name\":\"field1\",\"directions\":\"select * from table_name where value = %s\",\"srcName\":\"field2\", \"destName\":\"field_1\","
        + "\"multipleRetrieval\": true}" + "]}";

    GrammarParser.getGrammar(grammarJsonRepr);

  }

  @Test(expected = NullPointerException.class)
  public void testSemanticallyIncorrectGrammarRepr() {
    final String grammarJsonRepr =
        "{\"fields\":[" + "{\"name\":\"field1\",\"srcName\":\"field2\", \"destName\":\"field_1\"},"
            + "{\"multipleRetrieval\": true}" + "]}";

    GrammarParser.getGrammar(grammarJsonRepr);
  }

}
