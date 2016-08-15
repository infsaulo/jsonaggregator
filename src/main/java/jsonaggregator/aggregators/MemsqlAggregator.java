package jsonaggregator.aggregators;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.wizzardo.tools.json.JsonItem;
import com.wizzardo.tools.json.JsonObject;

import jsonaggregator.grammar.Field;

public class MemsqlAggregator implements Aggregator, Serializable {

  private static final long serialVersionUID = 6177079076132389890L;

  final Connection conn;

  public MemsqlAggregator(final String url, final String user, final String pass)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

    Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
    conn = DriverManager.getConnection(url, user, pass);
  }

  @Override
  public List<Object> retrieveField(final Field field, final JsonObject json) {

    List<Object> listResult = new ArrayList<>();

    if (field.isMultipleRetrieval()) {

      for (final JsonItem item : json.getAsJsonArray(field.getSrcName())) {
        final String value = item.asString();
        final String query = String.format(field.getDirections(), value);

        try {

          listResult.addAll(retrieveFiedlValue(query));
        } catch (final SQLException e) {
          e.printStackTrace();
        }
      }

    } else {

      final String value = json.getAsString(field.getSrcName());
      final String query = String.format(field.getDirections(), value);

      try {

        listResult = retrieveFiedlValue(query);
      } catch (final SQLException e) {
        e.printStackTrace();
      }
    }

    return listResult;
  }

  private List<Object> retrieveFiedlValue(final String query) throws SQLException {

    final List<Object> resultList = new ArrayList<>();
    final Statement stmt = conn.createStatement();
    final ResultSet results = stmt.executeQuery(query);

    while (results.next()) {
      final Object obj = results.getObject(1);
      resultList.add(obj);
    }

    return resultList;
  }

}
