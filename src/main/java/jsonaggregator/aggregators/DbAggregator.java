package jsonaggregator.aggregators;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.wizzardo.tools.json.JsonObject;

import jsonaggregator.grammar.Field;

public interface DbAggregator {

  public List<Object> retrieveField(Connection conn, Field toAggregateField, JsonObject json)
      throws SQLException;
}
