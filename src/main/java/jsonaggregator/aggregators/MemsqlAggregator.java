package jsonaggregator.aggregators;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.wizzardo.tools.json.JsonItem;
import com.wizzardo.tools.json.JsonObject;

import jsonaggregator.grammar.Field;

public class MemsqlAggregator implements DbAggregator, Serializable {

  private static final long serialVersionUID = 97581030976788024L;

  @Override
  public List<Object> retrieveField(final Connection conn, final Field field, final JsonObject json)
      throws SQLException {

    List<Object> listResult = new ArrayList<>();

    if (field.isMultipleRetrieval()) {

      for (final JsonItem item : json.getAsJsonArray(field.getSrcName())) {

        final String value = item.asString();
        final String query = String.format(field.getDirections(), value);

        listResult.addAll(retrieveFiedlValue(conn, query));
      }

    } else {

      final String value = json.getAsString(field.getSrcName());
      final String query = String.format(field.getDirections(), value);

      listResult = retrieveFiedlValue(conn, query);
    }

    return listResult;
  }

  private List<Object> retrieveFiedlValue(final Connection conn, final String query) {

    final List<Object> resultList = new ArrayList<>();
    Statement stmt = null;
    ResultSet results = null;

    try {
      stmt = conn.createStatement();
      results = stmt.executeQuery(query);

      while (results.next()) {

        final Object obj = results.getObject(1);
        resultList.add(obj);
      }
    } catch (final SQLException e) {
      e.printStackTrace();
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (final SQLException e) {
          e.printStackTrace();
        }
      }
      if (results != null) {
        try {
          results.close();
        } catch (final SQLException e) {
          e.printStackTrace();
        }
      }
    }
    return resultList;
  }

}
