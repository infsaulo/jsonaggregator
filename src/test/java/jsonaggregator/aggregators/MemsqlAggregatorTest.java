package jsonaggregator.aggregators;

import java.sql.SQLException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;

import jsonaggregator.grammar.Field;
import jsonaggregator.grammar.Grammar;
import jsonaggregator.grammar.GrammarParser;

public class MemsqlAggregatorTest {

  @Test
  public void testRetrieveFieldFromMemsql() {

    final String grammarRepr = "{\"fields\":[{\"name\":cluster, \"srcName\":\"address\", "
        + "\"directions\":\"select cluster_id from address_cluster_ids where address = X'%s'\"}]}";

    final String jsonRepr = "{\"address\":\"00000000001509001a01ea8cd3dde67bfeee9d59\"}";

    final JsonObject jsonObj = JsonTools.parse(jsonRepr).asJsonObject();
    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    try {

      final MemsqlAggregator fieldAggregator = new MemsqlAggregator(
          "jdbc:mysql://104.197.1.101:3307/blockstem", "saulo", "1J/x=/[DO{p&|~+1");

      for (final Field field : grammar.getFields()) {

        final List<Object> resultList = fieldAggregator.retrieveField(field, jsonObj);
        Assert.assertTrue(!resultList.isEmpty());
      }
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException
        | SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMultipleRetrieveFieldFromMemsql() {

    final String grammarRepr =
        "{\"fields\":[{\"name\":cluster, \"srcName\":\"addresses\", \"multipleRetrieval\":true, "
            + "\"directions\":\"select cluster_id from address_cluster_ids where address = X'%s'\"}]}";

    final String jsonRepr = "{\"addresses\":[\"00000000001509001a01ea8cd3dde67bfeee9d59\","
        + "\"000000009568fb2da418e440bb7286fcc53615e4\", \"0000005df0b5002894f92c59de900e3a5d1550bf\"]}";

    final JsonObject jsonObj = JsonTools.parse(jsonRepr).asJsonObject();
    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    try {

      final MemsqlAggregator fieldAggregator = new MemsqlAggregator(
          "jdbc:mysql://104.197.1.101:3307/blockstem", "saulo", "1J/x=/[DO{p&|~+1");

      for (final Field field : grammar.getFields()) {

        final List<Object> resultList = fieldAggregator.retrieveField(field, jsonObj);
        Assert.assertTrue(!resultList.isEmpty());
      }
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException
        | SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testLoopRequestsRetrieveFieldFromMemsql() {

    final String grammarRepr =
        "{\"fields\":[{\"name\":cluster, \"srcName\":\"addresses\", \"multipleRetrieval\":true, "
            + "\"directions\":\"select cluster_id from address_cluster_ids where address = X'%s'\"}]}";

    final String jsonRepr = "{\"addresses\":[\"00000000001509001a01ea8cd3dde67bfeee9d59\","
        + "\"000000009568fb2da418e440bb7286fcc53615e4\", \"0000005df0b5002894f92c59de900e3a5d1550bf\"]}";

    final JsonObject jsonObj = JsonTools.parse(jsonRepr).asJsonObject();
    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    try {

      final MemsqlAggregator fieldAggregator = new MemsqlAggregator(
          "jdbc:mysql://104.197.1.101:3307/blockstem", "saulo", "1J/x=/[DO{p&|~+1");

      for (int indexTrial = 0; indexTrial < 100; indexTrial++) {
        for (final Field field : grammar.getFields()) {

          final List<Object> resultList = fieldAggregator.retrieveField(field, jsonObj);
          Assert.assertTrue(!resultList.isEmpty());
        }
      }
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException
        | SQLException e) {
      e.printStackTrace();
    }
  }

}
