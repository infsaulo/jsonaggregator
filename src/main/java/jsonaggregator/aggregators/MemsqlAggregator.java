package jsonaggregator.aggregators;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class MemsqlAggregator implements Aggregator, Serializable {

  private static final long serialVersionUID = 6177079076132389890L;

  final Connection conn;

  public MemsqlAggregator(final String url, final String user, final String pass)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException {

    Class.forName("com.mysql.jdbc.Driver").newInstance();
    conn = DriverManager.getConnection(url, user, pass);
  }

  @Override
  public String aggregateFields(final String aggregationGrammar, final String rawJson) {


    return null;
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
