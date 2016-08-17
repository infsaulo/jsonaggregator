package jsonaggregator.aggregators;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.wizzardo.tools.json.JsonObject;
import com.wizzardo.tools.json.JsonTools;

import jsonaggregator.grammar.Field;
import jsonaggregator.grammar.Grammar;
import jsonaggregator.grammar.GrammarParser;

public class MemsqlAggregatorTest {

  static Connection conn;

  @BeforeClass
  public static void setupClass()
      throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {

    Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
    conn = DriverManager.getConnection(System.getProperty("DB_URL_TEST"),
        System.getProperty("DB_USER_TEST"), System.getProperty("DB_PASS_TEST"));
  }

  @Test
  public void testRetrieveFieldFromMemsql() throws SQLException {

    final String grammarRepr = "{\"fields\":[{\"name\":cluster, \"srcName\":\"address\", "
        + "\"directions\":\"select cluster_id from address_cluster_ids where address = X'%s'\"}]}";

    final String jsonRepr = "{\"address\":\"00000000001509001a01ea8cd3dde67bfeee9d59\"}";

    final JsonObject jsonObj = JsonTools.parse(jsonRepr).asJsonObject();
    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    final MemsqlAggregator fieldAggregator = new MemsqlAggregator();

    for (final Field field : grammar.getFields()) {

      final List<Object> resultList = fieldAggregator.retrieveField(conn, field, jsonObj);
      Assert.assertTrue(!resultList.isEmpty());
    }
  }

  @Test
  public void testMultipleRetrieveFieldFromMemsql() throws SQLException {

    final String grammarRepr =
        "{\"fields\":[{\"name\":cluster, \"srcName\":\"addresses\", \"multipleRetrieval\":true, "
            + "\"directions\":\"select cluster_id from address_cluster_ids where address = X'%s'\"}]}";

    final String jsonRepr = "{\"addresses\":[\"00000000001509001a01ea8cd3dde67bfeee9d59\","
        + "\"000000009568fb2da418e440bb7286fcc53615e4\", \"0000005df0b5002894f92c59de900e3a5d1550bf\"]}";

    final JsonObject jsonObj = JsonTools.parse(jsonRepr).asJsonObject();
    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    final MemsqlAggregator fieldAggregator = new MemsqlAggregator();

    for (final Field field : grammar.getFields()) {

      final List<Object> resultList = fieldAggregator.retrieveField(conn, field, jsonObj);
      Assert.assertTrue(!resultList.isEmpty());
    }
  }

  @Test
  public void testLoopRequestsRetrieveFieldFromMemsql() throws SQLException {

    final String grammarRepr =
        "{\"fields\":[{\"name\":cluster, \"srcName\":\"addresses\", \"multipleRetrieval\":true, "
            + "\"directions\":\"select cluster_id from address_cluster_ids where address = X'%s'\"}]}";

    final String jsonRepr = "{\"addresses\":[\"00000000001509001a01ea8cd3dde67bfeee9d59\","
        + "\"000000009568fb2da418e440bb7286fcc53615e4\", \"0000005df0b5002894f92c59de900e3a5d1550bf\"]}";

    final JsonObject jsonObj = JsonTools.parse(jsonRepr).asJsonObject();
    final Grammar grammar = GrammarParser.getGrammar(grammarRepr);

    final MemsqlAggregator fieldAggregator = new MemsqlAggregator();

    for (int indexTrial = 0; indexTrial < 100; indexTrial++) {
      for (final Field field : grammar.getFields()) {

        final List<Object> resultList = fieldAggregator.retrieveField(conn, field, jsonObj);
        Assert.assertTrue(!resultList.isEmpty());
      }
    }
  }

}
